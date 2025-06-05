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
	Long: `rollbackコマンドは指定されたS3バケットとオブジェクトを
指定された時間以前の最新バージョンにロールバックします。
--key または --prefix のいずれかを指定する必要があります。
--prefix を指定した場合は、そのプレフィックスに一致する全てのオブジェクトを並列でロールバックします。

指定された時間以降に変更がない場合は何もしません。
指定された時間以降に最初に作成された場合は削除します。
バージョニングが有効なバケットで使用できます。`,
	Run: func(cmd *cobra.Command, args []string) {
		bucket, _ := cmd.Flags().GetString("bucket")
		key, _ := cmd.Flags().GetString("key")
		prefix, _ := cmd.Flags().GetString("prefix")
		timestampStr, _ := cmd.Flags().GetString("timestamp")

		if bucket == "" || timestampStr == "" {
			slog.Error("必須パラメータが不足しています", "bucket", bucket, "timestamp", timestampStr)
			cmd.Help()
			return
		}

		if key == "" && prefix == "" {
			slog.Error("--key または --prefix のいずれかを指定する必要があります")
			cmd.Help()
			return
		}

		timestamp, err := time.Parse(time.RFC3339, timestampStr)
		if err != nil {
			slog.Error("タイムスタンプの形式が無効です", "error", err, "timestamp", timestampStr)
			slog.Info("有効な形式: YYYY-MM-DDThh:mm:ssZ (例: 2023-01-01T12:00:00Z)")
			return
		}

		if key != "" {
			slog.Info("単一オブジェクトのロールバック処理を開始します", "bucket", bucket, "key", key, "timestamp", timestamp.Format(time.RFC3339))
		} else {
			slog.Info("複数オブジェクトのロールバック処理を開始します", "bucket", bucket, "prefix", prefix, "timestamp", timestamp.Format(time.RFC3339))
		}
		
		opts := s3.RollbackOptions{
			Bucket:    bucket,
			Key:       key,
			Prefix:    prefix,
			Timestamp: timestamp,
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
	rollbackCmd.Flags().StringP("key", "k", "", "S3オブジェクトキー (--key または --prefix のいずれかが必須)")
	rollbackCmd.Flags().StringP("prefix", "p", "", "S3オブジェクトのプレフィックス (--key または --prefix のいずれかが必須)")
	rollbackCmd.Flags().StringP("timestamp", "t", "", "ロールバック先の時間 (ISO 8601形式: YYYY-MM-DDThh:mm:ssZ) (必須)")
	
	rollbackCmd.MarkFlagRequired("bucket")
	rollbackCmd.MarkFlagRequired("timestamp")
}
