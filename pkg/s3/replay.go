package s3

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ReplayOptions はリプレイのオプション
type ReplayOptions struct {
	SourceBucket      string    // 変更元のバケット
	DestBucket        string    // 変更先のバケット
	SourceFile        string    // 変更リストのファイルパス
	Concurrency       int       // 並列処理数
	SpeedFactor       float64   // 再生速度の倍率 (1.0 = 実時間、2.0 = 2倍速)
	DryRun            bool      // 実際に変更を適用せずに実行
	StartTime         time.Time // 開始時間（指定しない場合は現在時刻）
	IgnoreTimeWindows bool      // 時間間隔を無視して即時実行
}

// ReplayEvent はリプレイ中のイベントを表す構造体
type ReplayEvent struct {
	Change       ObjectChange
	ScheduledAt  time.Time
	ExecutedAt   time.Time
	Status       string
	ErrorMessage string
}

// ReplayResult はリプレイの結果を表す構造体
type ReplayResult struct {
	TotalEvents     int
	SuccessEvents   int
	FailedEvents    int
	SkippedEvents   int
	StartTime       time.Time
	EndTime         time.Time
	Events          []ReplayEvent
	DetailedResults bool
}

// Replay は変更リストを元にS3イベントを再現します
func Replay(opts ReplayOptions) (*ReplayResult, error) {
	// AWS設定の読み込み
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		slog.Error("AWS設定の読み込みに失敗しました", "error", err)
		return nil, fmt.Errorf("AWS設定の読み込みに失敗しました: %w", err)
	}

	// S3クライアントの作成
	client := s3.NewFromConfig(cfg)

	// 変更リストの読み込み
	changes, err := loadChangesFromFile(opts.SourceFile)
	if err != nil {
		return nil, err
	}

	// 変更リストを時間順にソート
	sort.Slice(changes, func(i, j int) bool {
		return changes[i].Timestamp.Before(changes[j].Timestamp)
	})

	slog.Info("変更リストを読み込みました", "count", len(changes))

	// 並列処理数のデフォルト値を設定
	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = 10
	}

	// 再生速度のデフォルト値を設定
	speedFactor := opts.SpeedFactor
	if speedFactor <= 0 {
		speedFactor = 1.0
	}

	// 開始時間のデフォルト値を設定
	startTime := opts.StartTime
	if startTime.IsZero() {
		startTime = time.Now()
	}

	// 結果の初期化
	result := &ReplayResult{
		TotalEvents:     len(changes),
		SuccessEvents:   0,
		FailedEvents:    0,
		SkippedEvents:   0,
		StartTime:       startTime,
		DetailedResults: true,
	}

	// 同一キーへの操作を直列化するためのマップ
	keyMutexes := sync.Map{}

	// イベントチャネル
	eventCh := make(chan ObjectChange, concurrency)

	// 完了チャネル
	doneCh := make(chan ReplayEvent, len(changes))

	// エラーチャネル
	errCh := make(chan error, 1)

	// 最初のイベントの時間
	var firstEventTime time.Time
	if len(changes) > 0 {
		firstEventTime = changes[0].Timestamp
	}

	// ワーカーゴルーチンを起動
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for change := range eventCh {
				// 同一キーへの操作を直列化するためのミューテックスを取得
				var mu sync.Mutex
				mutexIf, _ := keyMutexes.LoadOrStore(change.Key, &mu)
				mutex := mutexIf.(*sync.Mutex)

				// イベントの実行時間を計算
				var scheduledAt time.Time
				if opts.IgnoreTimeWindows {
					scheduledAt = time.Now()
				} else {
					// 最初のイベントからの相対時間を計算
					relativeTime := change.Timestamp.Sub(firstEventTime)
					// 再生速度を適用
					adjustedTime := time.Duration(float64(relativeTime) / speedFactor)
					// 開始時間からの相対時間を計算
					scheduledAt = startTime.Add(adjustedTime)
				}

				// 現在時刻がスケジュール時間より前なら待機
				if time.Now().Before(scheduledAt) && !opts.IgnoreTimeWindows {
					sleepTime := scheduledAt.Sub(time.Now())
					slog.Debug("イベント実行を待機します", "key", change.Key, "sleepTime", sleepTime)
					time.Sleep(sleepTime)
				}

				// 同一キーへの操作はロックを取得して直列化
				mutex.Lock()

				// イベントを実行
				event := ReplayEvent{
					Change:      change,
					ScheduledAt: scheduledAt,
					ExecutedAt:  time.Now(),
				}

				slog.Info("イベントを実行します", "key", change.Key, "changeType", change.ChangeType)

				if !opts.DryRun {
					err := executeChange(client, opts.SourceBucket, opts.DestBucket, change)
					if err != nil {
						event.Status = "FAILED"
						event.ErrorMessage = err.Error()
						slog.Error("イベントの実行に失敗しました", "key", change.Key, "error", err)
					} else {
						event.Status = "SUCCESS"
						slog.Info("イベントの実行が完了しました", "key", change.Key)
					}
				} else {
					event.Status = "DRYRUN"
					slog.Info("ドライラン: イベントをスキップしました", "key", change.Key)
				}

				// ロックを解放
				mutex.Unlock()

				// 結果を送信
				doneCh <- event
			}
		}()
	}

	// イベントをチャネルに送信
	go func() {
		for _, change := range changes {
			select {
			case eventCh <- change:
			case err := <-errCh:
				slog.Error("エラーが発生したため処理を中断します", "error", err)
				close(eventCh)
				return
			}
		}
		close(eventCh)
	}()

	// 結果を収集
	go func() {
		for i := 0; i < len(changes); i++ {
			event := <-doneCh
			switch event.Status {
			case "SUCCESS":
				result.SuccessEvents++
			case "FAILED":
				result.FailedEvents++
			case "DRYRUN":
				result.SkippedEvents++
			}
			result.Events = append(result.Events, event)
		}
		close(doneCh)
	}()

	// ワーカーの完了を待機
	wg.Wait()

	// 結果を返す
	result.EndTime = time.Now()
	return result, nil
}

// loadChangesFromFile はファイルから変更リストを読み込みます
func loadChangesFromFile(filePath string) ([]ObjectChange, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("ファイルのオープンに失敗しました: %w", err)
	}
	defer file.Close()

	var changes []ObjectChange
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&changes); err != nil {
		return nil, fmt.Errorf("JSONのデコードに失敗しました: %w", err)
	}

	return changes, nil
}

// executeChange は変更を実行します
func executeChange(client *s3.Client, sourceBucket, destBucket string, change ObjectChange) error {
	ctx := context.TODO()

	switch change.ChangeType {
	case ChangeTypeCreate, ChangeTypeUpdate:
		return copyObject(ctx, client, sourceBucket, destBucket, change)
	case ChangeTypeDelete:
		return deleteObject(ctx, client, destBucket, change)
	case ChangeTypeUndelete:
		return undeleteObject(ctx, client, sourceBucket, destBucket, change)
	default:
		return fmt.Errorf("不明な変更タイプです: %s", change.ChangeType)
	}
}

// copyObject はオブジェクトをコピーします
func copyObject(ctx context.Context, client *s3.Client, sourceBucket, destBucket string, change ObjectChange) error {
	// バージョンIDが指定されている場合はそのバージョンをコピー
	var copySource string
	if change.VersionID != "" {
		copySource = fmt.Sprintf("%s/%s?versionId=%s", sourceBucket, change.Key, change.VersionID)
	} else {
		copySource = fmt.Sprintf("%s/%s", sourceBucket, change.Key)
	}

	_, err := client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(destBucket),
		Key:        aws.String(change.Key),
		CopySource: aws.String(copySource),
	})

	if err != nil {
		return fmt.Errorf("オブジェクトのコピーに失敗しました: %w", err)
	}

	return nil
}

// deleteObject はオブジェクトを削除します
func deleteObject(ctx context.Context, client *s3.Client, destBucket string, change ObjectChange) error {
	_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(destBucket),
		Key:    aws.String(change.Key),
	})

	if err != nil {
		return fmt.Errorf("オブジェクトの削除に失敗しました: %w", err)
	}

	return nil
}

// undeleteObject は削除されたオブジェクトを復元します
func undeleteObject(ctx context.Context, client *s3.Client, sourceBucket, destBucket string, change ObjectChange) error {
	// 前のバージョンIDが指定されている場合はそのバージョンをコピー
	if change.PreviousVersionID == "" {
		return fmt.Errorf("復元するバージョンIDが指定されていません")
	}

	copySource := fmt.Sprintf("%s/%s?versionId=%s", sourceBucket, change.Key, change.PreviousVersionID)

	_, err := client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(destBucket),
		Key:        aws.String(change.Key),
		CopySource: aws.String(copySource),
	})

	if err != nil {
		return fmt.Errorf("オブジェクトの復元に失敗しました: %w", err)
	}

	return nil
}

// PrintReplayResult はリプレイ結果を出力します
func PrintReplayResult(result *ReplayResult, writer io.Writer) {
	duration := result.EndTime.Sub(result.StartTime)
	
	fmt.Fprintf(writer, "リプレイ結果:\n")
	fmt.Fprintf(writer, "  開始時間: %s\n", result.StartTime.Format(time.RFC3339))
	fmt.Fprintf(writer, "  終了時間: %s\n", result.EndTime.Format(time.RFC3339))
	fmt.Fprintf(writer, "  所要時間: %s\n", duration)
	fmt.Fprintf(writer, "  総イベント数: %d\n", result.TotalEvents)
	fmt.Fprintf(writer, "  成功: %d\n", result.SuccessEvents)
	fmt.Fprintf(writer, "  失敗: %d\n", result.FailedEvents)
	fmt.Fprintf(writer, "  スキップ: %d\n", result.SkippedEvents)
	
	if result.DetailedResults && len(result.Events) > 0 {
		fmt.Fprintf(writer, "\n詳細結果:\n")
		for i, event := range result.Events {
			if i >= 10 && len(result.Events) > 20 {
				fmt.Fprintf(writer, "  ... 省略 (%d件) ...\n", len(result.Events)-20)
				i = len(result.Events) - 10
				event = result.Events[i]
			}
			
			fmt.Fprintf(writer, "  %s - %s - %s - %s\n", 
				event.ExecutedAt.Format(time.RFC3339),
				event.Change.Key,
				event.Change.ChangeType,
				event.Status)
				
			if event.Status == "FAILED" {
				fmt.Fprintf(writer, "    エラー: %s\n", event.ErrorMessage)
			}
			
			if i >= len(result.Events) - 10 {
				break
			}
		}
	}
}
