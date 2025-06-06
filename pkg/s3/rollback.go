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
		slog.Info("バケット全体を対象としています", "bucket", opts.Bucket)
	} else {
		slog.Info("プレフィックスに一致するオブジェクトを対象としています", "bucket", opts.Bucket, "prefix", prefix)
	}

	return rollbackMultipleObjects(client, opts.Bucket, prefix, opts.Timestamp, opts.Concurrency)
}

// rollbackMultipleObjects はプレフィックスに一致する複数のオブジェクトを並列でロールバックします
func rollbackMultipleObjects(client *s3.Client, bucket, prefix string, timestamp time.Time, concurrency int) error {
	// プレフィックスに一致するオブジェクトの一覧を取得
	slog.Debug("オブジェクト一覧を取得しています", "bucket", bucket, "prefix", prefix)
	
	resp, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	
	if err != nil {
		slog.Error("オブジェクト一覧の取得に失敗しました", "error", err)
		return fmt.Errorf("オブジェクト一覧の取得に失敗しました: %w", err)
	}

	if len(resp.Contents) == 0 {
		slog.Info("対象オブジェクトが見つかりませんでした", "prefix", prefix)
		return nil
	}

	slog.Info("ロールバック処理を開始します", "対象数", len(resp.Contents), "並列数", concurrency)

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
				slog.Debug("オブジェクト処理開始", "worker", workerID, "key", key)
				err := rollbackSingleObject(client, bucket, key, timestamp)
				
				if err != nil {
					slog.Error("オブジェクト処理失敗", "worker", workerID, "key", key, "error", err)
					errCh <- fmt.Errorf("オブジェクト %s のロールバックに失敗しました: %w", key, err)
					return
				}
				
				slog.Debug("オブジェクト処理完了", "worker", workerID, "key", key)
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
	
	slog.Info("ロールバック処理が完了しました", "処理数", len(resp.Contents))
	return nil
}

// rollbackSingleObject は単一のオブジェクトをロールバックします
func rollbackSingleObject(client *s3.Client, bucket, key string, timestamp time.Time) error {
	// オブジェクトのバージョン一覧を取得
	slog.Debug("バージョン一覧取得", "bucket", bucket, "key", key)
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
		slog.Debug("オブジェクトが見つかりませんでした", "key", key)
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
			slog.Debug("指定時間以降の変更を検出", "key", key, "versionID", *v.VersionId, "lastModified", *v.LastModified)
		}
	}

	// 最初のバージョンが指定された時間以降に作成された場合
	if firstVersionTime != nil && !firstVersionTime.Before(timestamp) {
		isCreatedAfterTimestamp = true
		slog.Debug("指定時間以降に作成されたオブジェクト", "key", key, "firstVersionTime", *firstVersionTime)
	}

	// 指定された時間以降に変更がない場合はロールバック不要
	if !hasChangesAfterTimestamp {
		slog.Debug("変更なしのためスキップ", "key", key)
		return nil
	}

	// 指定された時間以降に最初に作成された場合は削除
	if isCreatedAfterTimestamp {
		slog.Debug("オブジェクト削除開始", "bucket", bucket, "key", key)
		_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			slog.Error("オブジェクトの削除に失敗しました", "key", key, "error", err)
			return fmt.Errorf("オブジェクトの削除に失敗しました: %w", err)
		}
		slog.Debug("オブジェクト削除完了", "key", key)
		return nil
	}

	// 指定された時間より前の最新バージョンを検索
	slog.Debug("過去バージョン検索", "key", key, "timestamp", timestamp)
	versionID, err := findVersionBeforeTimestamp(client, bucket, key, timestamp)
	if err != nil {
		slog.Error("バージョン検索に失敗しました", "key", key, "error", err)
		return err
	}
	slog.Debug("過去バージョン発見", "key", key, "versionID", versionID)
	
	return copySpecificVersion(client, bucket, key, versionID)
}

func copySpecificVersion(client *s3.Client, bucket, key, versionID string) error {
	slog.Debug("バージョンコピー開始", "bucket", bucket, "key", key, "versionID", versionID)
	_, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		CopySource: aws.String(fmt.Sprintf("%s/%s?versionId=%s", bucket, key, versionID)),
	})

	if err != nil {
		slog.Error("オブジェクトのコピーに失敗しました", "key", key, "error", err)
		return fmt.Errorf("オブジェクトのコピーに失敗しました: %w", err)
	}

	slog.Debug("バージョンコピー完了", "key", key)
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
			slog.Debug("対象バージョン検出", "key", key, "versionID", *v.VersionId, "lastModified", *v.LastModified)
			if latestVersionBeforeTimestamp == nil || v.LastModified.After(latestLastModified) {
				latestVersionBeforeTimestamp = v.VersionId
				latestLastModified = *v.LastModified
				slog.Debug("より新しいバージョン発見", "key", key, "versionID", *v.VersionId, "lastModified", *v.LastModified)
			}
		}
	}

	if latestVersionBeforeTimestamp == nil {
		slog.Error("指定された時間より前のバージョンが見つかりませんでした", "key", key, "timestamp", timestamp)
		return "", fmt.Errorf("指定された時間 %v より前のバージョンが見つかりませんでした", timestamp)
	}

	slog.Debug("最適バージョン決定", "key", key, "versionID", *latestVersionBeforeTimestamp, "lastModified", latestLastModified)
	return *latestVersionBeforeTimestamp, nil
}
