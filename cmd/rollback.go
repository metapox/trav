package cmd

import (
	"fmt"

	"github.com/metapox/trav/pkg/s3"
	"github.com/spf13/cobra"
)

// rollbackCmd はS3のロールバック操作を実行するコマンドを表します
var rollbackCmd = &cobra.Command{
	Use:   "rollback",
	Short: "S3のオブジェクトを以前のバージョンにロールバックします",
	Long: `rollbackコマンドは指定されたS3バケットとオブジェクトを
以前のバージョンにロールバックします。バージョニングが有効なバケットで使用できます。`,
	Run: func(cmd *cobra.Command, args []string) {
		// バケット名とオブジェクトキーを取得
		bucket, _ := cmd.Flags().GetString("bucket")
		key, _ := cmd.Flags().GetString("key")
		version, _ := cmd.Flags().GetString("version")

		// 必須パラメータのチェック
		if bucket == "" || key == "" {
			fmt.Println("エラー: バケット名とオブジェクトキーは必須です")
			cmd.Help()
			return
		}

		// ロールバック処理の実行
		fmt.Printf("バケット '%s' のオブジェクト '%s' をバージョン '%s' にロールバックします\n", 
			bucket, key, version)
		
		// S3ロールバック処理を実行
		opts := s3.RollbackOptions{
			Bucket:    bucket,
			Key:       key,
			VersionID: version,
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
	rollbackCmd.Flags().StringP("version", "v", "", "ロールバック先のバージョンID (指定がない場合は直前のバージョン)")
	
	// 必須フラグの設定
	rollbackCmd.MarkFlagRequired("bucket")
	rollbackCmd.MarkFlagRequired("key")
}
