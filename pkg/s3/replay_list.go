package s3

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
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
	Bucket    string
	Prefix    string
	Timestamp time.Time
}

// GetChangesList は指定された時間以降のオブジェクト変更リストを取得します
func GetChangesList(opts ReplayListOptions) ([]ObjectChange, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		slog.Error("AWS設定の読み込みに失敗しました", "error", err)
		return nil, fmt.Errorf("AWS設定の読み込みに失敗しました: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	// バケットのバージョニングが有効かチェック
	versioningResp, err := client.GetBucketVersioning(context.TODO(), &s3.GetBucketVersioningInput{
		Bucket: aws.String(opts.Bucket),
	})
	if err != nil {
		slog.Error("バケットのバージョニング設定の取得に失敗しました", "error", err)
		return nil, fmt.Errorf("バケットのバージョニング設定の取得に失敗しました: %w", err)
	}

	if versioningResp.Status != s3types.BucketVersioningStatusEnabled {
		slog.Warn("バケットのバージョニングが有効になっていません。完全な変更履歴を取得できない可能性があります")
	}

	// オブジェクトのバージョン一覧を取得
	slog.Info("バージョン一覧を取得します", "bucket", opts.Bucket, "prefix", opts.Prefix)
	
	var allVersions []s3types.ObjectVersion
	var allDeleteMarkers []s3types.DeleteMarkerEntry
	var continuationToken *string
	
	// ListObjectVersionsは1000件ずつしか返さないため、ページネーションで全て取得
	for {
		resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
			Bucket: aws.String(opts.Bucket),
			Prefix: aws.String(opts.Prefix),
			KeyMarker: continuationToken,
		})

		if err != nil {
			slog.Error("バージョン一覧の取得に失敗しました", "error", err)
			return nil, fmt.Errorf("バージョン一覧の取得に失敗しました: %w", err)
		}

		allVersions = append(allVersions, resp.Versions...)
		allDeleteMarkers = append(allDeleteMarkers, resp.DeleteMarkers...)
		
		// 次のページがなければ終了
		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break
		}
		
		continuationToken = resp.NextKeyMarker
	}
	
	slog.Info("バージョン一覧を取得しました", "versions", len(allVersions), "deleteMarkers", len(allDeleteMarkers))

	// 変更リストを作成
	var changes []ObjectChange
	
	// 通常のバージョンを処理
	for _, v := range allVersions {
		// 指定された時間以降のみ対象
		if !v.LastModified.Before(opts.Timestamp) {
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
	}
	
	// 削除マーカーを処理
	for _, dm := range allDeleteMarkers {
		// 指定された時間以降のみ対象
		if !dm.LastModified.Before(opts.Timestamp) {
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
	}
	
	// 時間順にソート
	sort.Slice(changes, func(i, j int) bool {
		return changes[i].Timestamp.Before(changes[j].Timestamp)
	})
	
	// 変更タイプの詳細な判定
	keyVersionMap := make(map[string][]ObjectChange)
	
	// キーごとにバージョンをグループ化
	for _, change := range changes {
		keyVersionMap[change.Key] = append(keyVersionMap[change.Key], change)
	}
	
	// 各キーについて変更タイプを判定
	var result []ObjectChange
	for key, keyChanges := range keyVersionMap {
		// 時間順にソート
		sort.Slice(keyChanges, func(i, j int) bool {
			return keyChanges[i].Timestamp.Before(keyChanges[j].Timestamp)
		})
		
		// キーの全バージョンを取得（指定時間より前も含む）
		allKeyVersions, err := getAllVersionsForKey(client, opts.Bucket, key)
		if err != nil {
			slog.Warn("キーの全バージョン取得に失敗しました", "key", key, "error", err)
		}
		
		// 前のバージョンIDを設定
		for i := range keyChanges {
			if i > 0 && !keyChanges[i].IsDeleteMarker {
				keyChanges[i].PreviousVersionID = keyChanges[i-1].VersionID
			} else if !keyChanges[i].IsDeleteMarker {
				// 最初の変更の場合、指定時間より前の最新バージョンを探す
				prevVersion := findLatestVersionBeforeTimestamp(allKeyVersions, keyChanges[i].Key, opts.Timestamp)
				if prevVersion != "" {
					keyChanges[i].PreviousVersionID = prevVersion
				}
			}
			
			// 変更タイプの詳細判定
			if i == 0 && isFirstVersionOfKey(allKeyVersions, keyChanges[i].VersionID) {
				keyChanges[i].ChangeType = ChangeTypeCreate
			} else if keyChanges[i].IsDeleteMarker {
				keyChanges[i].ChangeType = ChangeTypeDelete
			} else if i > 0 && keyChanges[i-1].IsDeleteMarker {
				keyChanges[i].ChangeType = ChangeTypeUndelete
			} else {
				keyChanges[i].ChangeType = ChangeTypeUpdate
			}
		}
		
		result = append(result, keyChanges...)
	}
	
	// 最終的に時間順にソート
	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp.Before(result[j].Timestamp)
	})
	
	slog.Info("変更リストを作成しました", "changes", len(result))
	return result, nil
}

// getAllVersionsForKey は指定されたキーの全バージョンを取得します
func getAllVersionsForKey(client *s3.Client, bucket, key string) ([]s3types.ObjectVersion, error) {
	resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
	})

	if err != nil {
		return nil, err
	}

	var versions []s3types.ObjectVersion
	for _, v := range resp.Versions {
		if *v.Key == key {
			versions = append(versions, v)
		}
	}

	return versions, nil
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
