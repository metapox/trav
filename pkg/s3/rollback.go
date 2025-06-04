package s3

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// RollbackOptions はロールバック操作のオプションを定義します
type RollbackOptions struct {
	Bucket    string
	Key       string
	VersionID string
}

// Rollback は指定されたS3オブジェクトを以前のバージョンにロールバックします
func Rollback(opts RollbackOptions) error {
	// AWS設定の読み込み
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return fmt.Errorf("AWS設定の読み込みに失敗しました: %w", err)
	}

	// S3クライアントの作成
	client := s3.NewFromConfig(cfg)

	// バージョンIDが指定されていない場合は、直前のバージョンを取得
	if opts.VersionID == "" {
		versionID, err := getLatestNonCurrentVersion(client, opts.Bucket, opts.Key)
		if err != nil {
			return err
		}
		opts.VersionID = versionID
	}

	// 指定されたバージョンを現在のオブジェクトにコピー
	_, err = client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:     aws.String(opts.Bucket),
		Key:        aws.String(opts.Key),
		CopySource: aws.String(fmt.Sprintf("%s/%s?versionId=%s", opts.Bucket, opts.Key, opts.VersionID)),
	})

	if err != nil {
		return fmt.Errorf("オブジェクトのコピーに失敗しました: %w", err)
	}

	return nil
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
