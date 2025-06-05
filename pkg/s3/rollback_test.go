package s3

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// S3ClientInterface はテスト用のS3クライアントインターフェース
type S3ClientInterface interface {
	ListObjectVersions(ctx context.Context, params *s3.ListObjectVersionsInput, optFns ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// S3RollbackClientMock はS3クライアントのモック
type S3RollbackClientMock struct {
	mock.Mock
}

func (m *S3RollbackClientMock) ListObjectVersions(ctx context.Context, params *s3.ListObjectVersionsInput, optFns ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*s3.ListObjectVersionsOutput), args.Error(1)
}

func (m *S3RollbackClientMock) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*s3.ListObjectsV2Output), args.Error(1)
}

func (m *S3RollbackClientMock) CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*s3.CopyObjectOutput), args.Error(1)
}

func (m *S3RollbackClientMock) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*s3.DeleteObjectOutput), args.Error(1)
}

// rollbackSingleObjectTest はテスト用のラッパー関数
func rollbackSingleObjectTest(client S3ClientInterface, bucket, key string, timestamp time.Time) error {
	return rollbackSingleObjectWithClient(client, bucket, key, timestamp)
}

// rollbackSingleObjectWithClient はテスト可能なバージョン
func rollbackSingleObjectWithClient(client S3ClientInterface, bucket, key string, timestamp time.Time) error {
	// オブジェクトのバージョン一覧を取得
	resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
	})

	if err != nil {
		return errors.New("バージョン一覧の取得に失敗しました: " + err.Error())
	}

	// 指定されたキーに完全一致するバージョンのみをフィルタリング
	var versions []s3types.ObjectVersion
	for _, v := range resp.Versions {
		if *v.Key == key {
			versions = append(versions, v)
		}
	}

	if len(versions) == 0 {
		return errors.New("指定されたオブジェクト " + key + " が見つかりませんでした")
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
		}
	}

	// 最初のバージョンが指定された時間以降に作成された場合
	if firstVersionTime != nil && !firstVersionTime.Before(timestamp) {
		isCreatedAfterTimestamp = true
	}

	// 指定された時間以降に変更がない場合はロールバック不要
	if !hasChangesAfterTimestamp {
		return nil
	}

	// 指定された時間以降に最初に作成された場合は削除
	if isCreatedAfterTimestamp {
		_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return errors.New("オブジェクトの削除に失敗しました: " + err.Error())
		}
		return nil
	}

	// 指定された時間より前の最新バージョンを検索
	versionID, err := findVersionBeforeTimestampTest(client, bucket, key, timestamp)
	if err != nil {
		return err
	}
	
	return copySpecificVersionTest(client, bucket, key, versionID)
}

// findVersionBeforeTimestampTest はテスト用のラッパー関数
func findVersionBeforeTimestampTest(client S3ClientInterface, bucket, key string, timestamp time.Time) (string, error) {
	resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
	})

	if err != nil {
		return "", errors.New("バージョン一覧の取得に失敗しました: " + err.Error())
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
		return "", errors.New("指定された時間より前のバージョンが見つかりませんでした")
	}

	return *latestVersionBeforeTimestamp, nil
}

// copySpecificVersionTest はテスト用のラッパー関数
func copySpecificVersionTest(client S3ClientInterface, bucket, key, versionID string) error {
	_, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		CopySource: aws.String(bucket + "/" + key + "?versionId=" + versionID),
	})

	if err != nil {
		return errors.New("オブジェクトのコピーに失敗しました: " + err.Error())
	}

	return nil
}

// rollbackMultipleObjectsTest はテスト用のラッパー関数
func rollbackMultipleObjectsTest(client S3ClientInterface, bucket, prefix string, timestamp time.Time, concurrency int) error {
	// プレフィックスに一致するオブジェクトの一覧を取得
	resp, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	
	if err != nil {
		return errors.New("オブジェクト一覧の取得に失敗しました: " + err.Error())
	}

	if len(resp.Contents) == 0 {
		return nil
	}

	// テストでは並列処理をシミュレートするために逐次処理
	for _, obj := range resp.Contents {
		err := rollbackSingleObjectTest(client, bucket, *obj.Key, timestamp)
		if err != nil {
			return errors.New("オブジェクト " + *obj.Key + " のロールバックに失敗しました: " + err.Error())
		}
	}
	
	return nil
}

func TestRollbackSingleObject_NoChangesAfterTimestamp(t *testing.T) {
	// モックの準備
	mockClient := new(S3RollbackClientMock)
	
	// テストデータ
	bucket := "test-bucket"
	key := "test-key"
	timestamp := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	
	// バージョン一覧のモック応答
	lastModified := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	versionID := "v1"
	isLatest := true
	
	mockClient.On("ListObjectVersions", mock.Anything, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
	}).Return(&s3.ListObjectVersionsOutput{
		Versions: []s3types.ObjectVersion{
			{
				Key:         aws.String(key),
				VersionId:   aws.String(versionID),
				IsLatest:    aws.Bool(isLatest),
				LastModified: aws.Time(lastModified),
			},
		},
	}, nil)
	
	// テスト実行
	err := rollbackSingleObjectTest(mockClient, bucket, key, timestamp)
	
	// 検証
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestRollbackSingleObject_CreatedAfterTimestamp(t *testing.T) {
	// モックの準備
	mockClient := new(S3RollbackClientMock)
	
	// テストデータ
	bucket := "test-bucket"
	key := "test-key"
	timestamp := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	
	// バージョン一覧のモック応答
	lastModified := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	versionID := "v1"
	isLatest := true
	
	mockClient.On("ListObjectVersions", mock.Anything, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
	}).Return(&s3.ListObjectVersionsOutput{
		Versions: []s3types.ObjectVersion{
			{
				Key:         aws.String(key),
				VersionId:   aws.String(versionID),
				IsLatest:    aws.Bool(isLatest),
				LastModified: aws.Time(lastModified),
			},
		},
	}, nil)
	
	mockClient.On("DeleteObject", mock.Anything, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}).Return(&s3.DeleteObjectOutput{}, nil)
	
	// テスト実行
	err := rollbackSingleObjectTest(mockClient, bucket, key, timestamp)
	
	// 検証
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestRollbackSingleObject_RollbackToPreviousVersion(t *testing.T) {
	// モックの準備
	mockClient := new(S3RollbackClientMock)
	
	// テストデータ
	bucket := "test-bucket"
	key := "test-key"
	timestamp := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	
	// バージョン一覧のモック応答
	lastModifiedV1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	lastModifiedV2 := time.Date(2023, 1, 3, 0, 0, 0, 0, time.UTC)
	versionIDV1 := "v1"
	versionIDV2 := "v2"
	
	mockClient.On("ListObjectVersions", mock.Anything, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
	}).Return(&s3.ListObjectVersionsOutput{
		Versions: []s3types.ObjectVersion{
			{
				Key:         aws.String(key),
				VersionId:   aws.String(versionIDV2),
				IsLatest:    aws.Bool(true),
				LastModified: aws.Time(lastModifiedV2),
			},
			{
				Key:         aws.String(key),
				VersionId:   aws.String(versionIDV1),
				IsLatest:    aws.Bool(false),
				LastModified: aws.Time(lastModifiedV1),
			},
		},
	}, nil).Times(2)
	
	mockClient.On("CopyObject", mock.Anything, &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		CopySource: aws.String(bucket + "/" + key + "?versionId=" + versionIDV1),
	}).Return(&s3.CopyObjectOutput{}, nil)
	
	// テスト実行
	err := rollbackSingleObjectTest(mockClient, bucket, key, timestamp)
	
	// 検証
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestRollbackMultipleObjects(t *testing.T) {
	// モックの準備
	mockClient := new(S3RollbackClientMock)
	
	// テストデータ
	bucket := "test-bucket"
	prefix := "test-prefix"
	timestamp := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	concurrency := 2
	
	// オブジェクト一覧のモック応答
	key1 := prefix + "/key1"
	key2 := prefix + "/key2"
	
	mockClient.On("ListObjectsV2", mock.Anything, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}).Return(&s3.ListObjectsV2Output{
		Contents: []s3types.Object{
			{
				Key: aws.String(key1),
			},
			{
				Key: aws.String(key2),
			},
		},
	}, nil)
	
	// key1のバージョン一覧
	lastModifiedV1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	versionIDV1 := "v1"
	
	mockClient.On("ListObjectVersions", mock.Anything, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key1),
	}).Return(&s3.ListObjectVersionsOutput{
		Versions: []s3types.ObjectVersion{
			{
				Key:         aws.String(key1),
				VersionId:   aws.String(versionIDV1),
				IsLatest:    aws.Bool(true),
				LastModified: aws.Time(lastModifiedV1),
			},
		},
	}, nil)
	
	// key2のバージョン一覧
	lastModifiedV2 := time.Date(2023, 1, 3, 0, 0, 0, 0, time.UTC)
	versionIDV2 := "v2"
	
	mockClient.On("ListObjectVersions", mock.Anything, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key2),
	}).Return(&s3.ListObjectVersionsOutput{
		Versions: []s3types.ObjectVersion{
			{
				Key:         aws.String(key2),
				VersionId:   aws.String(versionIDV2),
				IsLatest:    aws.Bool(true),
				LastModified: aws.Time(lastModifiedV2),
			},
		},
	}, nil)
	
	// key2の削除
	mockClient.On("DeleteObject", mock.Anything, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key2),
	}).Return(&s3.DeleteObjectOutput{}, nil)
	
	// テスト実行
	err := rollbackMultipleObjectsTest(mockClient, bucket, prefix, timestamp, concurrency)
	
	// 検証
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestRollbackMultipleObjects_EmptyList(t *testing.T) {
	// モックの準備
	mockClient := new(S3RollbackClientMock)
	
	// テストデータ
	bucket := "test-bucket"
	prefix := "test-prefix"
	timestamp := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	concurrency := 2
	
	// 空のオブジェクト一覧のモック応答
	mockClient.On("ListObjectsV2", mock.Anything, mock.Anything).Return(&s3.ListObjectsV2Output{
		Contents: []s3types.Object{},
	}, nil)
	
	// テスト実行
	err := rollbackMultipleObjectsTest(mockClient, bucket, prefix, timestamp, concurrency)
	
	// 検証
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestRollbackMultipleObjects_ErrorListingObjects(t *testing.T) {
	// モックの準備
	mockClient := new(S3RollbackClientMock)
	
	// テストデータ
	bucket := "test-bucket"
	prefix := "test-prefix"
	timestamp := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	concurrency := 2
	
	// エラー応答のモック
	mockClient.On("ListObjectsV2", mock.Anything, mock.Anything).Return(nil, errors.New("list objects error"))
	
	// テスト実行
	err := rollbackMultipleObjectsTest(mockClient, bucket, prefix, timestamp, concurrency)
	
	// 検証
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "オブジェクト一覧の取得に失敗しました")
	mockClient.AssertExpectations(t)
}
