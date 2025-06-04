package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// rootCmd はアプリケーションのルートコマンドを表します
var rootCmd = &cobra.Command{
	Use:   "trav",
	Short: "trav - S3イベントを再現するためのツール",
	Long: `trav はAmazon S3のイベントを再現するためのコマンドラインツールです。
様々なS3操作をシミュレートし、イベントを再現することができます。`,
	Run: func(cmd *cobra.Command, args []string) {
		// ルートコマンドが引数なしで実行された場合はヘルプを表示
		cmd.Help()
	},
}

// Execute はrootコマンドを実行し、エラーを処理します
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	// ここでフラグを設定したり、設定ファイルの読み込みなどの初期化処理を行います
}
