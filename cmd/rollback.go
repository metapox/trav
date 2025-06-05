package cmd

import (
	"log/slog"
	"time"

	"github.com/metapox/trav/pkg/s3"
	"github.com/spf13/cobra"
)

var rollbackCmd = &cobra.Command{
	Use:   "rollback",
	Short: "S3のオブジェクトを指定時間以前のバージョンにロールバックします",
	Long: `rollbackコマンドは指定されたS3バケットのオブジェクトを
指定された時間以前の最新バージョンにロールバックします。
--prefix を指定すると、そのプレフィックスに一致するオブジェクトのみを処理します。
--prefix を省略すると、バケット内の全てのオブジェクトを処理します。

指定された時間以降に変更がない場合は何もしません。
指定された時間以降に最初に作成された場合は削除します。
バージョニングが有効なバケットで使用できます。`,
	Run: func(cmd *cobra.Command, args []string) {
		bucket, _ := cmd.Flags().GetString("bucket")
		prefix, _ := cmd.Flags().GetString("prefix")
		timestampStr, _ := cmd.Flags().GetString("timestamp")
		concurrency, _ := cmd.Flags().GetInt("concurrency")

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

		if prefix == "" {
			slog.Info("バケット全体のロールバック処理を開始します", "bucket", bucket, "timestamp", timestamp.Format(time.RFC3339), "concurrency", concurrency)
		} else {
			slog.Info("プレフィックスに一致するオブジェクトのロールバック処理を開始します", "bucket", bucket, "prefix", prefix, "timestamp", timestamp.Format(time.RFC3339), "concurrency", concurrency)
		}
		
		opts := s3.RollbackOptions{
			Bucket:      bucket,
			Prefix:      prefix,
			Timestamp:   timestamp,
			Concurrency: concurrency,
		}
		
		if err := s3.Rollback(opts); err != nil {
			slog.Error("ロールバック処理中にエラーが発生しました", "error", err)
			return
		}
		
		slog.Info("処理が完了しました")
	},
}

func init() {
	rootCmd.AddCommand(rollbackCmd)

	rollbackCmd.Flags().StringP("bucket", "b", "", "S3バケット名 (必須)")
	rollbackCmd.Flags().StringP("prefix", "p", "", "S3オブジェクトのプレフィックス (省略時はバケット全体)")
	rollbackCmd.Flags().StringP("timestamp", "t", "", "ロールバック先の時間 (ISO 8601形式: YYYY-MM-DDThh:mm:ssZ) (必須)")
	rollbackCmd.Flags().IntP("concurrency", "c", 10, "並列処理数 (デフォルト: 10)")
	
	rollbackCmd.MarkFlagRequired("bucket")
	rollbackCmd.MarkFlagRequired("timestamp")
}
