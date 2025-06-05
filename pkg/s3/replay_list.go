package s3

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ChangeType は変更の種類を表す列挙型
type ChangeType string

const (
	ChangeTypeCreate    ChangeType = "CREATE"    // オブジェクトの作成
	ChangeTypeUpdate    ChangeType = "UPDATE"    // オブジェクトの更新
	ChangeTypeDelete    ChangeType = "DELETE"    // オブジェクトの削除
	ChangeTypeUndelete  ChangeType = "UNDELETE"  // 削除マーカーの削除（復元）
)

// ObjectChange はオブジェクトの変更を表す構造体
type ObjectChange struct {
	Key           string     `json:"key"`            // オブジェクトキー
	VersionID     string     `json:"versionId"`      // バージョンID
	ChangeType    ChangeType `json:"changeType"`     // 変更の種類
	Timestamp     time.Time  `json:"timestamp"`      // 変更時刻
	Size          int64      `json:"size,omitempty"` // オブジェクトサイズ（バイト）
	ETag          string     `json:"etag,omitempty"` // ETag
	IsDeleteMarker bool       `json:"isDeleteMarker"` // 削除マーカーかどうか
	PreviousVersionID string  `json:"previousVersionId,omitempty"` // 前のバージョンID
}

// ReplayListOptions は変更リスト取得のオプション
type ReplayListOptions struct {
	Bucket      string
	Prefix      string
	Timestamp   time.Time
	Concurrency int       // 並列処理数
	BatchSize   int       // バッチサイズ（一度に処理するオブジェクト数）
	Writer      ChangesWriter // 変更リストの書き込み先
}

// ChangesWriter は変更リストを書き込むインターフェース
type ChangesWriter interface {
	WriteChanges(changes []ObjectChange) error
	Close() error
}

// GetChangesList は指定された時間以降のオブジェクト変更リストを取得します
// 注意: この関数は後方互換性のために残していますが、大量のデータを処理する場合は
// ProcessChangesStreamingを使用することを推奨します
func GetChangesList(opts ReplayListOptions) ([]ObjectChange, error) {
	var changes []ObjectChange
	
	// 変更を受け取るコールバック関数
	callback := func(batch []ObjectChange) error {
		changes = append(changes, batch...)
		return nil
	}
	
	// 並列処理数とバッチサイズのデフォルト値を設定
	if opts.Concurrency <= 0 {
		opts.Concurrency = 10
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 1000
	}
	
	// ストリーミング処理を実行
	err := ProcessChangesStreaming(opts, callback)
	if err != nil {
		return nil, err
	}
	
	return changes, nil
}

// ProcessChangesStreaming は指定された時間以降のオブジェクト変更リストをストリーミング処理します
func ProcessChangesStreaming(opts ReplayListOptions, callback func([]ObjectChange) error) error {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		slog.Error("AWS設定の読み込みに失敗しました", "error", err)
		return fmt.Errorf("AWS設定の読み込みに失敗しました: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	// バケットのバージョニングが有効かチェック
	versioningResp, err := client.GetBucketVersioning(context.TODO(), &s3.GetBucketVersioningInput{
		Bucket: aws.String(opts.Bucket),
	})
	if err != nil {
		slog.Error("バケットのバージョニング設定の取得に失敗しました", "error", err)
		return fmt.Errorf("バケットのバージョニング設定の取得に失敗しました: %w", err)
	}

	if versioningResp.Status != s3types.BucketVersioningStatusEnabled {
		slog.Warn("バケットのバージョニングが有効になっていません。完全な変更履歴を取得できない可能性があります")
	}

	// 並列処理数のデフォルト値を設定
	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = 10
	}
	
	// バッチサイズのデフォルト値を設定
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	// オブジェクトのバージョン一覧を取得
	slog.Info("バージョン一覧を取得します", "bucket", opts.Bucket, "prefix", opts.Prefix)
	
	// キーのリストを取得
	keyList, err := listAllKeys(client, opts.Bucket, opts.Prefix)
	if err != nil {
		slog.Error("キー一覧の取得に失敗しました", "error", err)
		return fmt.Errorf("キー一覧の取得に失敗しました: %w", err)
	}
	
	slog.Info("キー一覧を取得しました", "keys", len(keyList))
	
	// キーを並列処理するためのチャネル
	keyCh := make(chan string, concurrency)
	
	// エラーチャネル
	errCh := make(chan error, 1)
	
	// 結果チャネル
	resultCh := make(chan []ObjectChange, concurrency)
	
	// 完了を通知するチャネル
	doneCh := make(chan struct{})
	
	// WaitGroup
	var wg sync.WaitGroup
	
	// ワーカーゴルーチンを起動
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			
			for key := range keyCh {
				// キーの変更リストを取得
				changes, err := getChangesForKey(client, opts.Bucket, key, opts.Timestamp)
				if err != nil {
					select {
					case errCh <- fmt.Errorf("キー %s の変更リスト取得に失敗しました: %w", key, err):
					default:
						// すでにエラーがある場合は無視
					}
					return
				}
				
				if len(changes) > 0 {
					resultCh <- changes
				}
			}
		}()
	}
	
	// 結果を収集するゴルーチン
	go func() {
		var batch []ObjectChange
		
		for changes := range resultCh {
			batch = append(batch, changes...)
			
			// バッチサイズに達したらコールバックを呼び出す
			if len(batch) >= batchSize {
				// 時間順にソート
				sort.Slice(batch, func(i, j int) bool {
					return batch[i].Timestamp.Before(batch[j].Timestamp)
				})
				
				if err := callback(batch); err != nil {
					select {
					case errCh <- err:
					default:
						// すでにエラーがある場合は無視
					}
				}
				
				// バッチをリセット
				batch = nil
			}
		}
		
		// 残りのバッチを処理
		if len(batch) > 0 {
			// 時間順にソート
			sort.Slice(batch, func(i, j int) bool {
				return batch[i].Timestamp.Before(batch[j].Timestamp)
			})
			
			if err := callback(batch); err != nil {
				select {
				case errCh <- err:
				default:
					// すでにエラーがある場合は無視
				}
			}
		}
		
		close(doneCh)
	}()
	
	// キーをチャネルに送信
	for _, key := range keyList {
		select {
		case keyCh <- key:
		case err := <-errCh:
			close(keyCh)
			return err
		}
	}
	
	// キーチャネルを閉じる
	close(keyCh)
	
	// ワーカーの完了を待機
	wg.Wait()
	
	// 結果チャネルを閉じる
	close(resultCh)
	
	// 結果収集の完了を待機
	select {
	case <-doneCh:
		// 正常終了
	case err := <-errCh:
		return err
	}
	
	// エラーがあれば返す
	select {
	case err := <-errCh:
		return err
	default:
		// エラーがなければ正常終了
	}
	
	slog.Info("変更リストの処理が完了しました")
	return nil
}

// listAllKeys はバケット内の全てのキーを取得します
func listAllKeys(client *s3.Client, bucket, prefix string) ([]string, error) {
	var keys []string
	var continuationToken *string
	
	for {
		resp, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
			Prefix: aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		
		if err != nil {
			return nil, err
		}
		
		for _, obj := range resp.Contents {
			keys = append(keys, *obj.Key)
		}
		
		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break
		}
		
		continuationToken = resp.NextContinuationToken
	}
	
	return keys, nil
}

// getChangesForKey は指定されたキーの変更リストを取得します
func getChangesForKey(client *s3.Client, bucket, key string, timestamp time.Time) ([]ObjectChange, error) {
	// キーの全バージョンを取得
	allKeyVersions, err := getAllVersionsForKey(client, bucket, key)
	if err != nil {
		return nil, err
	}
	
	// 指定された時間以降のバージョンをフィルタリング
	var filteredVersions []s3types.ObjectVersion
	var filteredDeleteMarkers []s3types.DeleteMarkerEntry
	
	for _, v := range allKeyVersions.Versions {
		if !v.LastModified.Before(timestamp) {
			filteredVersions = append(filteredVersions, v)
		}
	}
	
	for _, dm := range allKeyVersions.DeleteMarkers {
		if !dm.LastModified.Before(timestamp) {
			filteredDeleteMarkers = append(filteredDeleteMarkers, dm)
		}
	}
	
	// 変更リストを作成
	var changes []ObjectChange
	
	// 通常のバージョンを処理
	for _, v := range filteredVersions {
		change := ObjectChange{
			Key:        *v.Key,
			VersionID:  *v.VersionId,
			Timestamp:  *v.LastModified,
			IsDeleteMarker: false,
		}
		
		if v.Size != nil {
			change.Size = *v.Size
		}
		
		if v.ETag != nil {
			change.ETag = *v.ETag
		}
		
		// 変更タイプの判定（最初のバージョンかどうかは後で判定）
		change.ChangeType = ChangeTypeUpdate
		
		changes = append(changes, change)
		slog.Debug("バージョンを追加しました", "key", *v.Key, "versionId", *v.VersionId, "lastModified", *v.LastModified)
	}
	
	// 削除マーカーを処理
	for _, dm := range filteredDeleteMarkers {
		change := ObjectChange{
			Key:           *dm.Key,
			VersionID:     *dm.VersionId,
			ChangeType:    ChangeTypeDelete,
			Timestamp:     *dm.LastModified,
			IsDeleteMarker: true,
		}
		
		changes = append(changes, change)
		slog.Debug("削除マーカーを追加しました", "key", *dm.Key, "versionId", *dm.VersionId, "lastModified", *dm.LastModified)
	}
	
	// 時間順にソート
	sort.Slice(changes, func(i, j int) bool {
		return changes[i].Timestamp.Before(changes[j].Timestamp)
	})
	
	// 変更タイプの詳細な判定
	for i := range changes {
		if i == 0 {
			// 最初の変更の場合
			if isFirstVersionOfKey(allKeyVersions.Versions, changes[i].VersionID) {
				changes[i].ChangeType = ChangeTypeCreate
			} else if !changes[i].IsDeleteMarker {
				// 前のバージョンを探す
				prevVersion := findLatestVersionBeforeTimestamp(allKeyVersions.Versions, key, timestamp)
				if prevVersion != "" {
					changes[i].PreviousVersionID = prevVersion
				}
			}
		} else if !changes[i].IsDeleteMarker && i > 0 && changes[i-1].IsDeleteMarker {
			// 削除マーカーの後の変更は復元
			changes[i].ChangeType = ChangeTypeUndelete
			
			// 前のバージョンを探す（削除マーカーの前）
			for j := i - 2; j >= 0; j-- {
				if !changes[j].IsDeleteMarker {
					changes[i].PreviousVersionID = changes[j].VersionID
					break
				}
			}
		} else if !changes[i].IsDeleteMarker && i > 0 && !changes[i-1].IsDeleteMarker {
			// 通常の更新
			changes[i].PreviousVersionID = changes[i-1].VersionID
		}
	}
	
	return changes, nil
}

// KeyVersions はキーの全バージョン情報を保持する構造体
type KeyVersions struct {
	Versions      []s3types.ObjectVersion
	DeleteMarkers []s3types.DeleteMarkerEntry
}

// getAllVersionsForKey は指定されたキーの全バージョンを取得します
func getAllVersionsForKey(client *s3.Client, bucket, key string) (KeyVersions, error) {
	var result KeyVersions
	var continuationToken *string
	
	for {
		resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
			Bucket: aws.String(bucket),
			Prefix: aws.String(key),
			KeyMarker: continuationToken,
		})

		if err != nil {
			return result, err
		}

		// 指定されたキーに完全一致するバージョンのみをフィルタリング
		for _, v := range resp.Versions {
			if *v.Key == key {
				result.Versions = append(result.Versions, v)
			}
		}
		
		// 指定されたキーに完全一致する削除マーカーのみをフィルタリング
		for _, dm := range resp.DeleteMarkers {
			if *dm.Key == key {
				result.DeleteMarkers = append(result.DeleteMarkers, dm)
			}
		}
		
		// 次のページがなければ終了
		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break
		}
		
		continuationToken = resp.NextKeyMarker
	}

	return result, nil
}

// isFirstVersionOfKey は指定されたバージョンIDがキーの最初のバージョンかどうかを判定します
func isFirstVersionOfKey(versions []s3types.ObjectVersion, versionID string) bool {
	if len(versions) == 0 {
		return false
	}
	
	var oldestVersion *s3types.ObjectVersion
	var oldestTime time.Time
	
	for i, v := range versions {
		if i == 0 || v.LastModified.Before(oldestTime) {
			oldestVersion = &versions[i]
			oldestTime = *v.LastModified
		}
	}
	
	return oldestVersion != nil && *oldestVersion.VersionId == versionID
}

// findLatestVersionBeforeTimestamp は指定された時間より前の最新バージョンを探します
func findLatestVersionBeforeTimestamp(versions []s3types.ObjectVersion, key string, timestamp time.Time) string {
	var latestVersion string
	var latestTime time.Time
	
	for _, v := range versions {
		if *v.Key == key && v.LastModified.Before(timestamp) {
			if latestVersion == "" || v.LastModified.After(latestTime) {
				latestVersion = *v.VersionId
				latestTime = *v.LastModified
			}
		}
	}
	
	return latestVersion
}

// FileChangesWriter はファイルに変更リストを書き込むための構造体
type FileChangesWriter struct {
	file    *os.File
	encoder *json.Encoder
	first   bool
	mu      sync.Mutex
}

// NewFileChangesWriter は新しいFileChangesWriterを作成します
func NewFileChangesWriter(filePath string) (*FileChangesWriter, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	
	// JSONの配列開始を書き込む
	if _, err := file.Write([]byte("[\n")); err != nil {
		file.Close()
		return nil, err
	}
	
	return &FileChangesWriter{
		file:    file,
		encoder: json.NewEncoder(file),
		first:   true,
	}, nil
}

// WriteChanges は変更リストをファイルに書き込みます
func (w *FileChangesWriter) WriteChanges(changes []ObjectChange) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	for _, change := range changes {
		if !w.first {
			// 要素間のカンマを書き込む
			if _, err := w.file.Write([]byte(",\n")); err != nil {
				return err
			}
		} else {
			w.first = false
		}
		
		// インデントを追加
		if _, err := w.file.Write([]byte("  ")); err != nil {
			return err
		}
		
		// JSONエンコーダーはデフォルトで改行を追加するので、それを削除
		jsonData, err := json.Marshal(change)
		if err != nil {
			return err
		}
		
		if _, err := w.file.Write(jsonData); err != nil {
			return err
		}
	}
	
	return nil
}

// Close はファイルを閉じます
func (w *FileChangesWriter) Close() error {
	// JSONの配列終了を書き込む
	if _, err := w.file.Write([]byte("\n]\n")); err != nil {
		w.file.Close()
		return err
	}
	
	return w.file.Close()
}
