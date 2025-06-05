package s3

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type RollbackOptions struct {
	Bucket    string
	Key       string
	Timestamp time.Time
}

func Rollback(opts RollbackOptions) error {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		slog.Error("AWS設定の読み込みに失敗しました", "error", err)
		return fmt.Errorf("AWS設定の読み込みに失敗しました: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	// オブジェクトのバージョン一覧を取得
	slog.Info("バージョン一覧を取得します", "bucket", opts.Bucket, "key", opts.Key)
	resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(opts.Bucket),
		Prefix: aws.String(opts.Key),
	})

	if err != nil {
		slog.Error("バージョン一覧の取得に失敗しました", "error", err)
		return fmt.Errorf("バージョン一覧の取得に失敗しました: %w", err)
	}

	// 指定されたキーに完全一致するバージョンのみをフィルタリング
	var versions []s3types.ObjectVersion
	for _, v := range resp.Versions {
		if *v.Key == opts.Key {
			versions = append(versions, v)
		}
	}

	if len(versions) == 0 {
		slog.Error("指定されたオブジェクトが見つかりませんでした")
		return fmt.Errorf("指定されたオブジェクト %s が見つかりませんでした", opts.Key)
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
		if !v.LastModified.Before(opts.Timestamp) {
			hasChangesAfterTimestamp = true
			slog.Info("指定された時間以降の変更を検出しました", "versionID", *v.VersionId, "lastModified", *v.LastModified)
		}
	}

	// 最初のバージョンが指定された時間以降に作成された場合
	if firstVersionTime != nil && !firstVersionTime.Before(opts.Timestamp) {
		isCreatedAfterTimestamp = true
		slog.Info("オブジェクトは指定された時間以降に最初に作成されました", "firstVersionTime", *firstVersionTime)
	}

	// 指定された時間以降に変更がない場合はロールバック不要
	if !hasChangesAfterTimestamp {
		slog.Info("指定された時間以降に変更がないため、ロールバックは不要です")
		return nil
	}

	// 指定された時間以降に最初に作成された場合は削除
	if isCreatedAfterTimestamp {
		slog.Info("オブジェクトを削除します", "bucket", opts.Bucket, "key", opts.Key)
		_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(opts.Bucket),
			Key:    aws.String(opts.Key),
		})
		if err != nil {
			slog.Error("オブジェクトの削除に失敗しました", "error", err)
			return fmt.Errorf("オブジェクトの削除に失敗しました: %w", err)
		}
		slog.Info("オブジェクトの削除が完了しました")
		return nil
	}

	// 指定された時間より前の最新バージョンを検索
	slog.Info("指定時間以前のバージョンを検索します", "timestamp", opts.Timestamp)
	versionID, err := findVersionBeforeTimestamp(client, opts.Bucket, opts.Key, opts.Timestamp)
	if err != nil {
		slog.Error("バージョン検索に失敗しました", "error", err)
		return err
	}
	slog.Info("バージョンを見つけました", "versionID", versionID)
	
	return copySpecificVersion(client, opts.Bucket, opts.Key, versionID)
}

func copySpecificVersion(client *s3.Client, bucket, key, versionID string) error {
	slog.Info("オブジェクトのコピーを開始します", "bucket", bucket, "key", key, "versionID", versionID)
	_, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		CopySource: aws.String(fmt.Sprintf("%s/%s?versionId=%s", bucket, key, versionID)),
	})

	if err != nil {
		slog.Error("オブジェクトのコピーに失敗しました", "error", err)
		return fmt.Errorf("オブジェクトのコピーに失敗しました: %w", err)
	}

	slog.Info("オブジェクトのコピーが完了しました")
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
			slog.Debug("バージョンを検出しました", "versionID", *v.VersionId, "lastModified", *v.LastModified)
			if latestVersionBeforeTimestamp == nil || v.LastModified.After(latestLastModified) {
				latestVersionBeforeTimestamp = v.VersionId
				latestLastModified = *v.LastModified
				slog.Debug("より新しいバージョンを見つけました", "versionID", *v.VersionId, "lastModified", *v.LastModified)
			}
		}
	}

	if latestVersionBeforeTimestamp == nil {
		slog.Error("指定された時間より前のバージョンが見つかりませんでした", "timestamp", timestamp)
		return "", fmt.Errorf("指定された時間 %v より前のバージョンが見つかりませんでした", timestamp)
	}

	slog.Info("指定時間以前の最新バージョンを見つけました", "versionID", *latestVersionBeforeTimestamp, "lastModified", latestLastModified)
	return *latestVersionBeforeTimestamp, nil
}
