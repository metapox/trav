package cmd

import (
	"encoding/json"
	"fmt"
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
この出力は後でreplayコマンドで使用することができます。`,
	Run: func(cmd *cobra.Command, args []string) {
		bucket, _ := cmd.Flags().GetString("bucket")
		prefix, _ := cmd.Flags().GetString("prefix")
		timestampStr, _ := cmd.Flags().GetString("timestamp")
		outputFile, _ := cmd.Flags().GetString("output")

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
			Bucket:    bucket,
			Prefix:    prefix,
			Timestamp: timestamp,
		}
		
		changes, err := s3.GetChangesList(opts)
		if err != nil {
			slog.Error("変更リストの取得中にエラーが発生しました", "error", err)
			return
		}
		
		// 結果をJSON形式に変換
		jsonData, err := json.MarshalIndent(changes, "", "  ")
		if err != nil {
			slog.Error("JSON変換中にエラーが発生しました", "error", err)
			return
		}
		
		// 出力先の処理
		if outputFile != "" {
			// ファイルに出力
			err = os.WriteFile(outputFile, jsonData, 0644)
			if err != nil {
				slog.Error("ファイルへの書き込みに失敗しました", "file", outputFile, "error", err)
				return
			}
			slog.Info("変更リストをファイルに保存しました", "file", outputFile, "changes", len(changes))
		} else {
			// 標準出力に出力
			fmt.Println(string(jsonData))
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
	
	replayListCmd.MarkFlagRequired("bucket")
	replayListCmd.MarkFlagRequired("timestamp")
}
