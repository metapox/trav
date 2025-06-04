package s3

import (
	"context"
	"fmt"
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
		return fmt.Errorf("AWS設定の読み込みに失敗しました: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	versionID, err := findVersionBeforeTimestamp(client, opts.Bucket, opts.Key, opts.Timestamp)
	if err != nil {
		return err
	}
	return copySpecificVersion(client, opts.Bucket, opts.Key, versionID)
}

func copySpecificVersion(client *s3.Client, bucket, key, versionID string) error {
	_, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		CopySource: aws.String(fmt.Sprintf("%s/%s?versionId=%s", bucket, key, versionID)),
	})

	if err != nil {
		return fmt.Errorf("オブジェクトのコピーに失敗しました: %w", err)
	}

	return nil
}

func findVersionBeforeTimestamp(client *s3.Client, bucket, key string, timestamp time.Time) (string, error) {
	resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
	})

	if err != nil {
		return "", fmt.Errorf("バージョン一覧の取得に失敗しました: %w", err)
	}

	var latestVersionBeforeTimestamp *string
	var latestLastModified time.Time

	for _, v := range resp.Versions {
		if *v.Key == key && v.LastModified.Before(timestamp) {
			if latestVersionBeforeTimestamp == nil || v.LastModified.After(latestLastModified) {
				latestVersionBeforeTimestamp = v.VersionId
				latestLastModified = *v.LastModified
			}
		}
	}

	if latestVersionBeforeTimestamp == nil {
		return "", fmt.Errorf("指定された時間 %v より前のバージョンが見つかりませんでした", timestamp)
	}

	return *latestVersionBeforeTimestamp, nil
}
