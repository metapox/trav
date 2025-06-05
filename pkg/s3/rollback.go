package s3

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// デフォルトの並列処理数
const DefaultConcurrency = 10

type RollbackOptions struct {
	Bucket      string
	Prefix      string
	Timestamp   time.Time
	Concurrency int // 並列処理数
}

// Rollback は指定されたS3オブジェクトを指定時間以前のバージョンにロールバックします
func Rollback(opts RollbackOptions) error {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		slog.Error("AWS設定の読み込みに失敗しました", "error", err)
		return fmt.Errorf("AWS設定の読み込みに失敗しました: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	// 並列処理数が指定されていない場合はデフォルト値を使用
	if opts.Concurrency <= 0 {
		opts.Concurrency = DefaultConcurrency
	}

	// prefixが空の場合はバケット全体を対象とする
	prefix := opts.Prefix
	if prefix == "" {
		slog.Info("プレフィックスが指定されていないため、バケット全体を対象とします", "bucket", opts.Bucket)
	}

	return rollbackMultipleObjects(client, opts.Bucket, prefix, opts.Timestamp, opts.Concurrency)
}

// rollbackMultipleObjects はプレフィックスに一致する複数のオブジェクトを並列でロールバックします
func rollbackMultipleObjects(client *s3.Client, bucket, prefix string, timestamp time.Time, concurrency int) error {
	// プレフィックスに一致するオブジェクトの一覧を取得
	slog.Info("プレフィックスに一致するオブジェクトを検索します", "bucket", bucket, "prefix", prefix)
	
	resp, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	
	if err != nil {
		slog.Error("オブジェクト一覧の取得に失敗しました", "error", err)
		return fmt.Errorf("オブジェクト一覧の取得に失敗しました: %w", err)
	}

	if len(resp.Contents) == 0 {
		slog.Info("プレフィックスに一致するオブジェクトが見つかりませんでした", "prefix", prefix)
		return nil
	}

	slog.Info("ロールバック対象のオブジェクトを見つけました", "count", len(resp.Contents), "concurrency", concurrency)

	// エラーを格納するチャネル
	errCh := make(chan error, len(resp.Contents))
	
	// 処理するオブジェクトのキーを格納するチャネル
	keyCh := make(chan string, len(resp.Contents))
	
	// 全てのキーをチャネルに送信
	for _, obj := range resp.Contents {
		keyCh <- *obj.Key
	}
	close(keyCh)
	
	// WaitGroupで並列処理の完了を待機
	var wg sync.WaitGroup
	
	// 指定された並列数でワーカーを起動
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			// チャネルからキーを取得して処理
			for key := range keyCh {
				slog.Info("オブジェクトのロールバックを開始します", "worker", workerID, "key", key)
				err := rollbackSingleObject(client, bucket, key, timestamp)
				
				if err != nil {
					slog.Error("オブジェクトのロールバックに失敗しました", "worker", workerID, "key", key, "error", err)
					errCh <- fmt.Errorf("オブジェクト %s のロールバックに失敗しました: %w", key, err)
					return
				}
				
				slog.Info("オブジェクトのロールバックが完了しました", "worker", workerID, "key", key)
			}
		}(i)
	}
	
	// 全ての処理が完了するのを待機
	wg.Wait()
	close(errCh)
	
	// エラーがあれば最初のエラーを返す
	for err := range errCh {
		return err
	}
	
	slog.Info("全てのオブジェクトのロールバックが完了しました", "count", len(resp.Contents))
	return nil
}

// rollbackSingleObject は単一のオブジェクトをロールバックします
func rollbackSingleObject(client *s3.Client, bucket, key string, timestamp time.Time) error {
	// オブジェクトのバージョン一覧を取得
	slog.Info("バージョン一覧を取得します", "bucket", bucket, "key", key)
	resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
	})

	if err != nil {
		slog.Error("バージョン一覧の取得に失敗しました", "error", err)
		return fmt.Errorf("バージョン一覧の取得に失敗しました: %w", err)
	}

	// 指定されたキーに完全一致するバージョンのみをフィルタリング
	var versions []s3types.ObjectVersion
	for _, v := range resp.Versions {
		if *v.Key == key {
			versions = append(versions, v)
		}
	}

	if len(versions) == 0 {
		slog.Error("指定されたオブジェクトが見つかりませんでした", "key", key)
		return fmt.Errorf("指定されたオブジェクト %s が見つかりませんでした", key)
	}

	// 指定された時間以降に変更があるか確認
	var hasChangesAfterTimestamp bool
	var isCreatedAfterTimestamp bool
	var firstVersionTime *time.Time

	for _, v := range versions {
		// 最初のバージョンの時間を記録
		if firstVersionTime == nil || v.LastModified.Before(*firstVersionTime) {
			firstVersionTime = v.LastModified
		}

		// 指定された時間以降に変更があるか確認
		if !v.LastModified.Before(timestamp) {
			hasChangesAfterTimestamp = true
			slog.Info("指定された時間以降の変更を検出しました", "key", key, "versionID", *v.VersionId, "lastModified", *v.LastModified)
		}
	}

	// 最初のバージョンが指定された時間以降に作成された場合
	if firstVersionTime != nil && !firstVersionTime.Before(timestamp) {
		isCreatedAfterTimestamp = true
		slog.Info("オブジェクトは指定された時間以降に最初に作成されました", "key", key, "firstVersionTime", *firstVersionTime)
	}

	// 指定された時間以降に変更がない場合はロールバック不要
	if !hasChangesAfterTimestamp {
		slog.Info("指定された時間以降に変更がないため、ロールバックは不要です", "key", key)
		return nil
	}

	// 指定された時間以降に最初に作成された場合は削除
	if isCreatedAfterTimestamp {
		slog.Info("オブジェクトを削除します", "bucket", bucket, "key", key)
		_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			slog.Error("オブジェクトの削除に失敗しました", "key", key, "error", err)
			return fmt.Errorf("オブジェクトの削除に失敗しました: %w", err)
		}
		slog.Info("オブジェクトの削除が完了しました", "key", key)
		return nil
	}

	// 指定された時間より前の最新バージョンを検索
	slog.Info("指定時間以前のバージョンを検索します", "key", key, "timestamp", timestamp)
	versionID, err := findVersionBeforeTimestamp(client, bucket, key, timestamp)
	if err != nil {
		slog.Error("バージョン検索に失敗しました", "key", key, "error", err)
		return err
	}
	slog.Info("バージョンを見つけました", "key", key, "versionID", versionID)
	
	return copySpecificVersion(client, bucket, key, versionID)
}

func copySpecificVersion(client *s3.Client, bucket, key, versionID string) error {
	slog.Info("オブジェクトのコピーを開始します", "bucket", bucket, "key", key, "versionID", versionID)
	_, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		CopySource: aws.String(fmt.Sprintf("%s/%s?versionId=%s", bucket, key, versionID)),
	})

	if err != nil {
		slog.Error("オブジェクトのコピーに失敗しました", "key", key, "error", err)
		return fmt.Errorf("オブジェクトのコピーに失敗しました: %w", err)
	}

	slog.Info("オブジェクトのコピーが完了しました", "key", key)
	return nil
}

func findVersionBeforeTimestamp(client *s3.Client, bucket, key string, timestamp time.Time) (string, error) {
	resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
	})

	if err != nil {
		slog.Error("バージョン一覧の取得に失敗しました", "error", err)
		return "", fmt.Errorf("バージョン一覧の取得に失敗しました: %w", err)
	}

	var latestVersionBeforeTimestamp *string
	var latestLastModified time.Time

	for _, v := range resp.Versions {
		if *v.Key == key && v.LastModified.Before(timestamp) {
			slog.Debug("バージョンを検出しました", "key", key, "versionID", *v.VersionId, "lastModified", *v.LastModified)
			if latestVersionBeforeTimestamp == nil || v.LastModified.After(latestLastModified) {
				latestVersionBeforeTimestamp = v.VersionId
				latestLastModified = *v.LastModified
				slog.Debug("より新しいバージョンを見つけました", "key", key, "versionID", *v.VersionId, "lastModified", *v.LastModified)
			}
		}
	}

	if latestVersionBeforeTimestamp == nil {
		slog.Error("指定された時間より前のバージョンが見つかりませんでした", "key", key, "timestamp", timestamp)
		return "", fmt.Errorf("指定された時間 %v より前のバージョンが見つかりませんでした", timestamp)
	}

	slog.Info("指定時間以前の最新バージョンを見つけました", "key", key, "versionID", *latestVersionBeforeTimestamp, "lastModified", latestLastModified)
	return *latestVersionBeforeTimestamp, nil
}
