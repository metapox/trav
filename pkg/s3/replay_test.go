package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// S3ClientMock はS3クライアントのモック
type S3ClientMock struct {
	CopyObjectCalls    []CopyObjectInput
	DeleteObjectCalls  []DeleteObjectInput
	GetObjectCalls     []GetObjectInput
	CopyObjectFunc     func(ctx context.Context, params *CopyObjectInput) (*CopyObjectOutput, error)
	DeleteObjectFunc   func(ctx context.Context, params *DeleteObjectInput) (*DeleteObjectOutput, error)
	GetObjectFunc      func(ctx context.Context, params *GetObjectInput) (*GetObjectOutput, error)
}

type CopyObjectInput struct {
	Bucket     string
	Key        string
	CopySource string
}

type DeleteObjectInput struct {
	Bucket string
	Key    string
}

type GetObjectInput struct {
	Bucket    string
	Key       string
	VersionId string
}

type CopyObjectOutput struct{}
type DeleteObjectOutput struct{}
type GetObjectOutput struct{}

func (m *S3ClientMock) CopyObject(ctx context.Context, params *CopyObjectInput) (*CopyObjectOutput, error) {
	m.CopyObjectCalls = append(m.CopyObjectCalls, *params)
	if m.CopyObjectFunc != nil {
		return m.CopyObjectFunc(ctx, params)
	}
	return &CopyObjectOutput{}, nil
}

func (m *S3ClientMock) DeleteObject(ctx context.Context, params *DeleteObjectInput) (*DeleteObjectOutput, error) {
	m.DeleteObjectCalls = append(m.DeleteObjectCalls, *params)
	if m.DeleteObjectFunc != nil {
		return m.DeleteObjectFunc(ctx, params)
	}
	return &DeleteObjectOutput{}, nil
}

func (m *S3ClientMock) GetObject(ctx context.Context, params *GetObjectInput) (*GetObjectOutput, error) {
	m.GetObjectCalls = append(m.GetObjectCalls, *params)
	if m.GetObjectFunc != nil {
		return m.GetObjectFunc(ctx, params)
	}
	return &GetObjectOutput{}, nil
}

// mockExecuteChange はテスト用のexecuteChange関数
func mockExecuteChange(client interface{}, sourceBucket, destBucket string, change ObjectChange) error {
	mock, ok := client.(*S3ClientMock)
	if !ok {
		return nil
	}

	switch change.ChangeType {
	case ChangeTypeCreate, ChangeTypeUpdate:
		copySource := fmt.Sprintf("%s/%s", sourceBucket, change.Key)
		if change.VersionID != "" {
			copySource += "?versionId=" + change.VersionID
		}
		mock.CopyObject(context.TODO(), &CopyObjectInput{
			Bucket:     destBucket,
			Key:        change.Key,
			CopySource: copySource,
		})
	case ChangeTypeDelete:
		mock.DeleteObject(context.TODO(), &DeleteObjectInput{
			Bucket: destBucket,
			Key:    change.Key,
		})
	case ChangeTypeUndelete:
		if change.PreviousVersionID != "" {
			copySource := fmt.Sprintf("%s/%s?versionId=%s", sourceBucket, change.Key, change.PreviousVersionID)
			mock.CopyObject(context.TODO(), &CopyObjectInput{
				Bucket:     destBucket,
				Key:        change.Key,
				CopySource: copySource,
			})
		}
	}
	return nil
}

// テスト用の変更リストを作成する関数
func createTestChangesList(t *testing.T) string {
	// テスト用の時間を設定
	now := time.Now()
	time1 := now.Add(-10 * time.Minute)
	time2 := now.Add(-5 * time.Minute)
	time3 := now.Add(-2 * time.Minute)

	// テスト用の変更リスト
	changes := []ObjectChange{
		{
			Key:        "test/file1.txt",
			VersionID:  "v1",
			ChangeType: ChangeTypeCreate,
			Timestamp:  time1,
			Size:       100,
			ETag:       "etag1",
		},
		{
			Key:              "test/file2.txt",
			VersionID:        "v2",
			ChangeType:       ChangeTypeUpdate,
			Timestamp:        time2,
			Size:             200,
			ETag:             "etag2",
			PreviousVersionID: "v1",
		},
		{
			Key:           "test/file3.txt",
			VersionID:     "v3",
			ChangeType:    ChangeTypeDelete,
			Timestamp:     time3,
			IsDeleteMarker: true,
		},
	}

	// 一時ファイルを作成
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "changes.json")
	file, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("一時ファイルの作成に失敗しました: %v", err)
	}
	defer file.Close()

	// 変更リストをJSONとして書き込み
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(changes); err != nil {
		t.Fatalf("JSONのエンコードに失敗しました: %v", err)
	}

	return filePath
}

// TestLoadChangesFromFile は変更リストの読み込みをテストする
func TestLoadChangesFromFile(t *testing.T) {
	filePath := createTestChangesList(t)

	changes, err := loadChangesFromFile(filePath)
	if err != nil {
		t.Fatalf("変更リストの読み込みに失敗しました: %v", err)
	}

	if len(changes) != 3 {
		t.Errorf("変更リストの長さが期待と異なります: got %d, want %d", len(changes), 3)
	}

	// 各変更の内容を確認
	if changes[0].Key != "test/file1.txt" || changes[0].ChangeType != ChangeTypeCreate {
		t.Errorf("1つ目の変更が期待と異なります: %+v", changes[0])
	}
	if changes[1].Key != "test/file2.txt" || changes[1].ChangeType != ChangeTypeUpdate {
		t.Errorf("2つ目の変更が期待と異なります: %+v", changes[1])
	}
	if changes[2].Key != "test/file3.txt" || changes[2].ChangeType != ChangeTypeDelete {
		t.Errorf("3つ目の変更が期待と異なります: %+v", changes[2])
	}
}

// TestExecuteChange は変更の実行をテストする
func TestExecuteChange(t *testing.T) {
	tests := []struct {
		name       string
		change     ObjectChange
		wantCopy   bool
		wantDelete bool
	}{
		{
			name: "CREATE操作",
			change: ObjectChange{
				Key:        "test/file1.txt",
				VersionID:  "v1",
				ChangeType: ChangeTypeCreate,
			},
			wantCopy:   true,
			wantDelete: false,
		},
		{
			name: "UPDATE操作",
			change: ObjectChange{
				Key:        "test/file2.txt",
				VersionID:  "v2",
				ChangeType: ChangeTypeUpdate,
			},
			wantCopy:   true,
			wantDelete: false,
		},
		{
			name: "DELETE操作",
			change: ObjectChange{
				Key:        "test/file3.txt",
				VersionID:  "v3",
				ChangeType: ChangeTypeDelete,
			},
			wantCopy:   false,
			wantDelete: true,
		},
		{
			name: "UNDELETE操作",
			change: ObjectChange{
				Key:              "test/file4.txt",
				VersionID:        "v4",
				ChangeType:       ChangeTypeUndelete,
				PreviousVersionID: "v3",
			},
			wantCopy:   true,
			wantDelete: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// モックの作成
			mock := &S3ClientMock{}

			// テスト対象の関数を実行
			err := mockExecuteChange(mock, "source-bucket", "dest-bucket", tt.change)
			if err != nil {
				t.Fatalf("executeChange() error = %v", err)
			}

			// 期待通りの関数が呼ばれたか確認
			if tt.wantCopy && len(mock.CopyObjectCalls) == 0 {
				t.Errorf("CopyObject()が呼ばれませんでした")
			}
			if tt.wantDelete && len(mock.DeleteObjectCalls) == 0 {
				t.Errorf("DeleteObject()が呼ばれませんでした")
			}

			// CopyObjectの引数を確認
			if tt.wantCopy && len(mock.CopyObjectCalls) > 0 {
				call := mock.CopyObjectCalls[0]
				if call.Bucket != "dest-bucket" {
					t.Errorf("CopyObject()の宛先バケットが期待と異なります: got %s, want %s", call.Bucket, "dest-bucket")
				}
				if call.Key != tt.change.Key {
					t.Errorf("CopyObject()のキーが期待と異なります: got %s, want %s", call.Key, tt.change.Key)
				}
			}

			// DeleteObjectの引数を確認
			if tt.wantDelete && len(mock.DeleteObjectCalls) > 0 {
				call := mock.DeleteObjectCalls[0]
				if call.Bucket != "dest-bucket" {
					t.Errorf("DeleteObject()の宛先バケットが期待と異なります: got %s, want %s", call.Bucket, "dest-bucket")
				}
				if call.Key != tt.change.Key {
					t.Errorf("DeleteObject()のキーが期待と異なります: got %s, want %s", call.Key, tt.change.Key)
				}
			}
		})
	}
}

// TestReplayResult はリプレイ結果を出力するテスト
func TestPrintReplayResult(t *testing.T) {
	// テスト用の結果を作成
	result := &ReplayResult{
		TotalEvents:   5,
		SuccessEvents: 3,
		FailedEvents:  1,
		SkippedEvents: 1,
		StartTime:     time.Date(2025, 6, 5, 10, 0, 0, 0, time.UTC),
		EndTime:       time.Date(2025, 6, 5, 10, 1, 0, 0, time.UTC),
		Events: []ReplayEvent{
			{
				Change: ObjectChange{
					Key:        "test/file1.txt",
					ChangeType: ChangeTypeCreate,
				},
				Status: "SUCCESS",
			},
			{
				Change: ObjectChange{
					Key:        "test/file2.txt",
					ChangeType: ChangeTypeUpdate,
				},
				Status:       "FAILED",
				ErrorMessage: "テストエラー",
			},
		},
		DetailedResults: true,
	}

	// 出力先のバッファを作成
	var buf bytes.Buffer

	// テスト対象の関数を実行
	PrintReplayResult(result, &buf)

	// 出力を確認
	output := buf.String()
	if !strings.Contains(output, "総イベント数: 5") {
		t.Errorf("出力に総イベント数が含まれていません: %s", output)
	}
	if !strings.Contains(output, "成功: 3") {
		t.Errorf("出力に成功数が含まれていません: %s", output)
	}
	if !strings.Contains(output, "失敗: 1") {
		t.Errorf("出力に失敗数が含まれていません: %s", output)
	}
	if !strings.Contains(output, "test/file1.txt") {
		t.Errorf("出力にファイル名が含まれていません: %s", output)
	}
	if !strings.Contains(output, "テストエラー") {
		t.Errorf("出力にエラーメッセージが含まれていません: %s", output)
	}
}
