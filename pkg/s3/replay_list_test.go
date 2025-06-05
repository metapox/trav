package s3

import (
	"testing"
	"time"
	
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestFindLatestVersionBeforeTimestamp(t *testing.T) {
	// テスト用のタイムスタンプ
	now := time.Now()
	oneHourAgo := now.Add(-1 * time.Hour)
	twoHoursAgo := now.Add(-2 * time.Hour)
	threeHoursAgo := now.Add(-3 * time.Hour)

	// テスト用のバージョンリスト
	createVersionWithTime := func(key string, versionID string, timestamp time.Time) s3types.ObjectVersion {
		return s3types.ObjectVersion{
			Key:         &key,
			VersionId:   &versionID,
			LastModified: &timestamp,
		}
	}

	key := "test-key"
	versions := []s3types.ObjectVersion{
		createVersionWithTime(key, "v1", threeHoursAgo),
		createVersionWithTime(key, "v2", twoHoursAgo),
		createVersionWithTime(key, "v3", oneHourAgo),
		createVersionWithTime(key, "v4", now),
	}

	// テストケース
	tests := []struct {
		name      string
		timestamp time.Time
		expected  string
	}{
		{
			name:      "指定時間より前のバージョンがある場合",
			timestamp: oneHourAgo.Add(1 * time.Minute),
			expected:  "v3",
		},
		{
			name:      "指定時間と同じタイムスタンプのバージョンがある場合（その時間は含まない）",
			timestamp: oneHourAgo,
			expected:  "v2",
		},
		{
			name:      "指定時間より前のバージョンがない場合",
			timestamp: threeHoursAgo.Add(-1 * time.Minute),
			expected:  "",
		},
		{
			name:      "境界値: 最も古いバージョンのちょうど後",
			timestamp: threeHoursAgo.Add(1 * time.Millisecond),
			expected:  "v1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findLatestVersionBeforeTimestamp(versions, key, tt.timestamp)
			if result != tt.expected {
				t.Errorf("findLatestVersionBeforeTimestamp() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIsFirstVersionOfKey(t *testing.T) {
	// テスト用のタイムスタンプ
	now := time.Now()
	oneHourAgo := now.Add(-1 * time.Hour)
	twoHoursAgo := now.Add(-2 * time.Hour)

	// テスト用のバージョンリスト
	createVersionWithTime := func(key string, versionID string, timestamp time.Time) s3types.ObjectVersion {
		return s3types.ObjectVersion{
			Key:         &key,
			VersionId:   &versionID,
			LastModified: &timestamp,
		}
	}

	key := "test-key"
	versions := []s3types.ObjectVersion{
		createVersionWithTime(key, "v1", twoHoursAgo),
		createVersionWithTime(key, "v2", oneHourAgo),
		createVersionWithTime(key, "v3", now),
	}

	// テストケース
	tests := []struct {
		name      string
		versionID string
		expected  bool
	}{
		{
			name:      "最初のバージョンの場合",
			versionID: "v1",
			expected:  true,
		},
		{
			name:      "最初のバージョンでない場合",
			versionID: "v2",
			expected:  false,
		},
		{
			name:      "存在しないバージョンIDの場合",
			versionID: "non-existent",
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isFirstVersionOfKey(versions, tt.versionID)
			if result != tt.expected {
				t.Errorf("isFirstVersionOfKey() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// 空のバージョンリストのテスト
func TestEmptyVersions(t *testing.T) {
	emptyVersions := []s3types.ObjectVersion{}
	
	// findLatestVersionBeforeTimestamp
	result1 := findLatestVersionBeforeTimestamp(emptyVersions, "key", time.Now())
	if result1 != "" {
		t.Errorf("findLatestVersionBeforeTimestamp() with empty versions = %v, want %v", result1, "")
	}
	
	// isFirstVersionOfKey
	result2 := isFirstVersionOfKey(emptyVersions, "version")
	if result2 != false {
		t.Errorf("isFirstVersionOfKey() with empty versions = %v, want %v", result2, false)
	}
}
