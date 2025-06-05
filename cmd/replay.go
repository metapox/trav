package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/metapox/trav/pkg/s3"
	"github.com/spf13/cobra"
)

var replayCmd = &cobra.Command{
	Use:   "replay",
	Short: "変更リストを元にS3イベントを再現します",
	Long: `replayコマンドは、replay-listコマンドで生成された変更リストを元に、
S3イベントを再現します。

イベントは元の時間間隔を保ちながら実行され、同一ファイルへの操作は
直列で処理されます。異なるファイルへの操作は並列で処理されます。

--speed-factorオプションで再生速度を調整できます。例えば、2.0を指定すると
2倍速で再生されます。

--dry-runオプションを指定すると、実際に変更を適用せずに実行できます。`,
	Run: func(cmd *cobra.Command, args []string) {
		sourceBucket, _ := cmd.Flags().GetString("source-bucket")
		destBucket, _ := cmd.Flags().GetString("dest-bucket")
		sourceFile, _ := cmd.Flags().GetString("source-file")
		concurrency, _ := cmd.Flags().GetInt("concurrency")
		speedFactor, _ := cmd.Flags().GetFloat64("speed-factor")
		dryRun, _ := cmd.Flags().GetBool("dry-run")
		ignoreTimeWindows, _ := cmd.Flags().GetBool("ignore-time-windows")

		if sourceFile == "" {
			slog.Error("必須パラメータが不足しています", "source-file", sourceFile)
			cmd.Help()
			return
		}

		if destBucket == "" {
			slog.Error("必須パラメータが不足しています", "dest-bucket", destBucket)
			cmd.Help()
			return
		}

		// ソースバケットが指定されていない場合は宛先バケットと同じにする
		if sourceBucket == "" {
			sourceBucket = destBucket
		}

		slog.Info("リプレイを開始します", 
			"sourceFile", sourceFile, 
			"sourceBucket", sourceBucket, 
			"destBucket", destBucket, 
			"concurrency", concurrency, 
			"speedFactor", speedFactor, 
			"dryRun", dryRun,
			"ignoreTimeWindows", ignoreTimeWindows)

		opts := s3.ReplayOptions{
			SourceBucket:      sourceBucket,
			DestBucket:        destBucket,
			SourceFile:        sourceFile,
			Concurrency:       concurrency,
			SpeedFactor:       speedFactor,
			DryRun:            dryRun,
			StartTime:         time.Now(),
			IgnoreTimeWindows: ignoreTimeWindows,
		}

		result, err := s3.Replay(opts)
		if err != nil {
			slog.Error("リプレイ中にエラーが発生しました", "error", err)
			return
		}

		// 結果を出力
		s3.PrintReplayResult(result, os.Stdout)

		if result.FailedEvents > 0 {
			slog.Error("リプレイが完了しましたが、一部のイベントが失敗しました", 
				"total", result.TotalEvents, 
				"success", result.SuccessEvents, 
				"failed", result.FailedEvents, 
				"skipped", result.SkippedEvents)
			return
		}

		if dryRun {
			slog.Info("ドライランが完了しました", 
				"total", result.TotalEvents, 
				"skipped", result.SkippedEvents)
		} else {
			slog.Info("リプレイが完了しました", 
				"total", result.TotalEvents, 
				"success", result.SuccessEvents)
		}

		// 詳細な結果をファイルに出力するオプションを追加することも可能
		outputFile, _ := cmd.Flags().GetString("output")
		if outputFile != "" {
			file, err := os.Create(outputFile)
			if err != nil {
				slog.Error("結果ファイルの作成に失敗しました", "file", outputFile, "error", err)
				return
			}
			defer file.Close()

			fmt.Fprintf(file, "リプレイ詳細結果\n")
			fmt.Fprintf(file, "実行日時: %s\n\n", time.Now().Format(time.RFC3339))
			s3.PrintReplayResult(result, file)
			slog.Info("詳細結果をファイルに保存しました", "file", outputFile)
		}
	},
}

func init() {
	rootCmd.AddCommand(replayCmd)

	replayCmd.Flags().StringP("source-file", "f", "", "変更リストのファイルパス (必須)")
	replayCmd.Flags().StringP("source-bucket", "s", "", "変更元のバケット (指定しない場合は宛先バケットと同じ)")
	replayCmd.Flags().StringP("dest-bucket", "b", "", "変更先のバケット (必須)")
	replayCmd.Flags().IntP("concurrency", "c", 10, "並列処理数")
	replayCmd.Flags().Float64P("speed-factor", "x", 1.0, "再生速度の倍率 (1.0 = 実時間、2.0 = 2倍速)")
	replayCmd.Flags().BoolP("dry-run", "n", false, "実際に変更を適用せずに実行")
	replayCmd.Flags().Bool("ignore-time-windows", false, "時間間隔を無視して即時実行")
	replayCmd.Flags().StringP("output", "o", "", "詳細結果の出力ファイルパス")

	replayCmd.MarkFlagRequired("source-file")
	replayCmd.MarkFlagRequired("dest-bucket")
}
