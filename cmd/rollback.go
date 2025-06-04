package cmd

import (
	"fmt"
	"time"

	"github.com/metapox/trav/pkg/s3"
	"github.com/spf13/cobra"
)

// rollbackCmd はS3のロールバック操作を実行するコマンドを表します
var rollbackCmd = &cobra.Command{
	Use:   "rollback",
	Short: "S3のオブジェクトを指定時間以前のバージョンにロールバックします",
	Long: `rollbackコマンドは指定されたS3バケットとオブジェクトを
指定された時間以前の最新バージョンにロールバックします。
バージョニングが有効なバケットで使用できます。`,
	Run: func(cmd *cobra.Command, args []string) {
		// バケット名とオブジェクトキーを取得
		bucket, _ := cmd.Flags().GetString("bucket")
		key, _ := cmd.Flags().GetString("key")
		version, _ := cmd.Flags().GetString("version")
		timestampStr, _ := cmd.Flags().GetString("timestamp")

		// 必須パラメータのチェック
		if bucket == "" || key == "" {
			fmt.Println("エラー: バケット名とオブジェクトキーは必須です")
			cmd.Help()
			return
		}

		// タイムスタンプの解析
		var timestamp time.Time
		var err error
		if timestampStr != "" {
			// ISO 8601形式（YYYY-MM-DDThh:mm:ssZ）でパース
			timestamp, err = time.Parse(time.RFC3339, timestampStr)
			if err != nil {
				fmt.Printf("エラー: タイムスタンプの形式が無効です: %v\n", err)
				fmt.Println("有効な形式: YYYY-MM-DDThh:mm:ssZ (例: 2023-01-01T12:00:00Z)")
				return
			}
		}

		// ロールバック処理の実行
		if version != "" {
			fmt.Printf("バケット '%s' のオブジェクト '%s' をバージョン '%s' にロールバックします\n", 
				bucket, key, version)
		} else if !timestamp.IsZero() {
			fmt.Printf("バケット '%s' のオブジェクト '%s' を時間 '%s' 以前の最新バージョンにロールバックします\n", 
				bucket, key, timestamp.Format(time.RFC3339))
		} else {
			fmt.Printf("バケット '%s' のオブジェクト '%s' を直前のバージョンにロールバックします\n", 
				bucket, key)
		}
		
		// S3ロールバック処理を実行
		opts := s3.RollbackOptions{
			Bucket:    bucket,
			Key:       key,
			VersionID: version,
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

	// フラグの設定
	rollbackCmd.Flags().StringP("bucket", "b", "", "S3バケット名 (必須)")
	rollbackCmd.Flags().StringP("key", "k", "", "S3オブジェクトキー (必須)")
	rollbackCmd.Flags().StringP("version", "v", "", "ロールバック先の特定バージョンID")
	rollbackCmd.Flags().StringP("timestamp", "t", "", "ロールバック先の時間 (ISO 8601形式: YYYY-MM-DDThh:mm:ssZ)")
	
	// 必須フラグの設定
	rollbackCmd.MarkFlagRequired("bucket")
	rollbackCmd.MarkFlagRequired("key")
}
