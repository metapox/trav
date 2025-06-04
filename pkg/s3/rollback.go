package s3

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

	slog.Info("バージョン検索を開始します", "bucket", opts.Bucket, "key", opts.Key, "timestamp", opts.Timestamp)
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
	slog.Info("バージョン一覧を取得します", "bucket", bucket, "key", key)
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

	slog.Info("指定時間以前のバージョンを検索します", "timestamp", timestamp)
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
