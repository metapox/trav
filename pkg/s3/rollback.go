package s3

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// RollbackOptions はロールバック操作のオプションを定義します
type RollbackOptions struct {
	Bucket    string
	Key       string
	VersionID string
	Timestamp time.Time // 指定された時間
}

// Rollback は指定された時間以降の変更をその直前のバージョンに戻します
func Rollback(opts RollbackOptions) error {
	// AWS設定の読み込み
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return fmt.Errorf("AWS設定の読み込みに失敗しました: %w", err)
	}

	// S3クライアントの作成
	client := s3.NewFromConfig(cfg)

	// バージョンIDが指定されている場合は、そのバージョンを使用
	if opts.VersionID != "" {
		return copySpecificVersion(client, opts.Bucket, opts.Key, opts.VersionID)
	}

	// 時間が指定されている場合は、その時間直前のバージョンを検索
	if !opts.Timestamp.IsZero() {
		versionID, err := findVersionBeforeTimestamp(client, opts.Bucket, opts.Key, opts.Timestamp)
		if err != nil {
			return err
		}
		return copySpecificVersion(client, opts.Bucket, opts.Key, versionID)
	}

	// どちらも指定されていない場合は、直前のバージョンを使用
	versionID, err := getLatestNonCurrentVersion(client, opts.Bucket, opts.Key)
	if err != nil {
		return err
	}
	return copySpecificVersion(client, opts.Bucket, opts.Key, versionID)
}

// copySpecificVersion は指定されたバージョンを現在のオブジェクトにコピーします
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

// findVersionBeforeTimestamp は指定された時間より前の最新バージョンを検索します
func findVersionBeforeTimestamp(client *s3.Client, bucket, key string, timestamp time.Time) (string, error) {
	// オブジェクトのバージョン一覧を取得
	resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
	})

	if err != nil {
		return "", fmt.Errorf("バージョン一覧の取得に失敗しました: %w", err)
	}

	// キーに完全一致し、指定された時間より前のバージョンを探す
	var latestVersionBeforeTimestamp *string
	var latestLastModified time.Time

	for _, v := range resp.Versions {
		if *v.Key == key && v.LastModified.Before(timestamp) {
			// まだ最新バージョンが見つかっていないか、このバージョンがより新しい場合
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

// getLatestNonCurrentVersion は指定されたオブジェクトの最新の非現行バージョンを取得します
func getLatestNonCurrentVersion(client *s3.Client, bucket, key string) (string, error) {
	// オブジェクトのバージョン一覧を取得
	resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
	})

	if err != nil {
		return "", fmt.Errorf("バージョン一覧の取得に失敗しました: %w", err)
	}

	// キーに完全一致するバージョンを探す
	var versions []string
	for _, v := range resp.Versions {
		if *v.Key == key && !*v.IsLatest {
			versions = append(versions, *v.VersionId)
		}
	}

	if len(versions) == 0 {
		return "", fmt.Errorf("ロールバック可能な以前のバージョンが見つかりませんでした")
	}

	// 最初のバージョン（最新の非現行バージョン）を返す
	return versions[0], nil
}
