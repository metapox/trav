package cmd

import (
	"fmt"
	"time"

	"github.com/metapox/trav/pkg/s3"
	"github.com/spf13/cobra"
)

var rollbackCmd = &cobra.Command{
	Use:   "rollback",
	Short: "S3のオブジェクトを指定時間以前のバージョンにロールバックします",
	Long: `rollbackコマンドは指定されたS3バケットとオブジェクトを
指定された時間以前の最新バージョンにロールバックします。
バージョニングが有効なバケットで使用できます。`,
	Run: func(cmd *cobra.Command, args []string) {
		bucket, _ := cmd.Flags().GetString("bucket")
		key, _ := cmd.Flags().GetString("key")
		timestampStr, _ := cmd.Flags().GetString("timestamp")

		if bucket == "" || key == "" || timestampStr == "" {
			fmt.Println("エラー: バケット名、オブジェクトキー、タイムスタンプは必須です")
			cmd.Help()
			return
		}

		timestamp, err := time.Parse(time.RFC3339, timestampStr)
		if err != nil {
			fmt.Printf("エラー: タイムスタンプの形式が無効です: %v\n", err)
			fmt.Println("有効な形式: YYYY-MM-DDThh:mm:ssZ (例: 2023-01-01T12:00:00Z)")
			return
		}

		fmt.Printf("バケット '%s' のオブジェクト '%s' を時間 '%s' 以前の最新バージョンにロールバックします\n", 
			bucket, key, timestamp.Format(time.RFC3339))
		
		opts := s3.RollbackOptions{
			Bucket:    bucket,
			Key:       key,
			Timestamp: timestamp,
		}
		
		if err := s3.Rollback(opts); err != nil {
			fmt.Printf("ロールバック処理中にエラーが発生しました: %v\n", err)
			return
		}
		
		fmt.Println("ロールバック処理が完了しました")
	},
}

func init() {
	rootCmd.AddCommand(rollbackCmd)

	rollbackCmd.Flags().StringP("bucket", "b", "", "S3バケット名 (必須)")
	rollbackCmd.Flags().StringP("key", "k", "", "S3オブジェクトキー (必須)")
	rollbackCmd.Flags().StringP("timestamp", "t", "", "ロールバック先の時間 (ISO 8601形式: YYYY-MM-DDThh:mm:ssZ) (必須)")
	
	rollbackCmd.MarkFlagRequired("bucket")
	rollbackCmd.MarkFlagRequired("key")
	rollbackCmd.MarkFlagRequired("timestamp")
}
