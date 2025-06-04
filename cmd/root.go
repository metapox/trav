package cmd

import (
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "trav",
	Short: "trav - S3イベントを再現するためのツール",
	Long: `trav はAmazon S3のイベントを再現するためのコマンドラインツールです。
様々なS3操作をシミュレートし、イベントを再現することができます。`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func Execute() {
	// ログの初期化
	setupLogger()

	if err := rootCmd.Execute(); err != nil {
		slog.Error("コマンド実行中にエラーが発生しました", "error", err)
		os.Exit(1)
	}
}

func setupLogger() {
	// デバッグモードの取得
	debug, _ := rootCmd.PersistentFlags().GetBool("debug")
	
	// ログレベルの設定
	var logLevel slog.Level
	if debug {
		logLevel = slog.LevelDebug
	} else {
		logLevel = slog.LevelInfo
	}
	
	// ハンドラーの設定
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	})
	
	// グローバルロガーの設定
	slog.SetDefault(slog.New(handler))
	
	if debug {
		slog.Debug("デバッグモードが有効になりました")
	}
}

func init() {
	// グローバルフラグの設定
	rootCmd.PersistentFlags().BoolP("debug", "d", false, "デバッグモードを有効にする")
}
