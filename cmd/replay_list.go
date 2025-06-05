package cmd

import (
	"log/slog"
	"os"
	"time"

	"github.com/metapox/trav/pkg/s3"
	"github.com/spf13/cobra"
)

var replayListCmd = &cobra.Command{
	Use:   "replay-list",
	Short: "指定時間以降のS3オブジェクトの変更を取得します",
	Long: `replay-listコマンドは指定されたS3バケットとプレフィックスに対して、
指定された時間以降（その時間を含む）の変更を取得し、
後でリプレイしやすいフォーマットで出力します。

出力はJSONフォーマットで、各オブジェクトの変更履歴が含まれます。
この出力は後でreplayコマンドで使用することができます。

大量のオブジェクトを処理する場合は、--concurrencyオプションで並列処理数を
--batch-sizeオプションでバッチサイズを調整することができます。`,
	Run: func(cmd *cobra.Command, args []string) {
		bucket, _ := cmd.Flags().GetString("bucket")
		prefix, _ := cmd.Flags().GetString("prefix")
		timestampStr, _ := cmd.Flags().GetString("timestamp")
		outputFile, _ := cmd.Flags().GetString("output")
		concurrency, _ := cmd.Flags().GetInt("concurrency")
		batchSize, _ := cmd.Flags().GetInt("batch-size")

		if bucket == "" || timestampStr == "" {
			slog.Error("必須パラメータが不足しています", "bucket", bucket, "timestamp", timestampStr)
			cmd.Help()
			return
		}

		timestamp, err := time.Parse(time.RFC3339, timestampStr)
		if err != nil {
			slog.Error("タイムスタンプの形式が無効です", "error", err, "timestamp", timestampStr)
			slog.Info("有効な形式: YYYY-MM-DDThh:mm:ssZ (例: 2023-01-01T12:00:00Z)")
			return
		}

		slog.Info("変更リストの取得を開始します", "bucket", bucket, "prefix", prefix, "timestamp", timestamp.Format(time.RFC3339))
		
		opts := s3.ReplayListOptions{
			Bucket:      bucket,
			Prefix:      prefix,
			Timestamp:   timestamp,
			Concurrency: concurrency,
			BatchSize:   batchSize,
		}
		
		// 出力先の設定
		var writer s3.ChangesWriter
		
		if outputFile != "" {
			// ファイルに出力
			fileWriter, err := s3.NewFileChangesWriter(outputFile)
			if err != nil {
				slog.Error("出力ファイルの作成に失敗しました", "file", outputFile, "error", err)
				return
			}
			defer fileWriter.Close()
			writer = fileWriter
			
			// ストリーミング処理を実行
			err = s3.ProcessChangesStreaming(opts, func(changes []s3.ObjectChange) error {
				return writer.WriteChanges(changes)
			})
			
			if err != nil {
				slog.Error("変更リストの処理中にエラーが発生しました", "error", err)
				return
			}
			
			slog.Info("変更リストをファイルに保存しました", "file", outputFile)
		} else {
			// メモリに全て読み込んでから標準出力に出力
			changes, err := s3.GetChangesList(opts)
			if err != nil {
				slog.Error("変更リストの取得中にエラーが発生しました", "error", err)
				return
			}
			
			// 一時ファイルに書き込んでから標準出力にコピー
			tempFile, err := os.CreateTemp("", "trav-changes-*.json")
			if err != nil {
				slog.Error("一時ファイルの作成に失敗しました", "error", err)
				return
			}
			defer os.Remove(tempFile.Name())
			
			fileWriter, err := s3.NewFileChangesWriter(tempFile.Name())
			if err != nil {
				slog.Error("一時ファイルの作成に失敗しました", "error", err)
				return
			}
			
			if err := fileWriter.WriteChanges(changes); err != nil {
				slog.Error("一時ファイルへの書き込みに失敗しました", "error", err)
				fileWriter.Close()
				return
			}
			
			fileWriter.Close()
			
			// 一時ファイルを標準出力にコピー
			data, err := os.ReadFile(tempFile.Name())
			if err != nil {
				slog.Error("一時ファイルの読み込みに失敗しました", "error", err)
				return
			}
			
			os.Stdout.Write(data)
			slog.Info("変更リストを標準出力に出力しました", "changes", len(changes))
		}
	},
}

func init() {
	rootCmd.AddCommand(replayListCmd)

	replayListCmd.Flags().StringP("bucket", "b", "", "S3バケット名 (必須)")
	replayListCmd.Flags().StringP("prefix", "p", "", "S3オブジェクトのプレフィックス")
	replayListCmd.Flags().StringP("timestamp", "t", "", "取得開始時間 (ISO 8601形式: YYYY-MM-DDThh:mm:ssZ) (必須)")
	replayListCmd.Flags().StringP("output", "o", "", "出力ファイルパス (指定しない場合は標準出力)")
	replayListCmd.Flags().IntP("concurrency", "c", 10, "並列処理数")
	replayListCmd.Flags().Int("batch-size", 1000, "バッチサイズ (一度に処理するオブジェクト数)")
	
	replayListCmd.MarkFlagRequired("bucket")
	replayListCmd.MarkFlagRequired("timestamp")
}
