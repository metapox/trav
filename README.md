# trav

S3イベントを再現するためのコマンドラインツールです。

## インストール

```bash
go install github.com/metapox/trav@latest
```

または、リポジトリをクローンしてビルドすることもできます：

```bash
git clone https://github.com/metapox/trav.git
cd trav
go build -o trav
```

## 使い方

### rollbackコマンド

S3オブジェクトを指定時間以前のバージョンにロールバックします。

```bash
# 単一オブジェクトをロールバック
trav rollback --bucket バケット名 --key オブジェクトキー --timestamp 2023-01-01T12:00:00Z

# プレフィックスに一致する全てのオブジェクトを並列でロールバック
trav rollback --bucket バケット名 --prefix プレフィックス --timestamp 2023-01-01T12:00:00Z

# 並列処理数を指定してロールバック
trav rollback --bucket バケット名 --prefix プレフィックス --timestamp 2023-01-01T12:00:00Z --concurrency 20

# デバッグモードを有効にして実行
trav rollback --bucket バケット名 --key オブジェクトキー --timestamp 2023-01-01T12:00:00Z --debug
```

#### オプション

- `-b, --bucket` (必須): S3バケット名
- `-k, --key`: S3オブジェクトキー (--key または --prefix のいずれかが必須)
- `-p, --prefix`: S3オブジェクトのプレフィックス (--key または --prefix のいずれかが必須)
- `-t, --timestamp` (必須): ロールバック先の時間 (ISO 8601形式: YYYY-MM-DDThh:mm:ssZ)
- `-c, --concurrency`: 並列処理数 (デフォルト: 10)
- `-d, --debug`: デバッグモードを有効にする

#### 動作

- 指定された時間以降に変更がない場合は何もしません
- 指定された時間以降に最初に作成された場合は削除します
- それ以外の場合は、指定された時間より前の最新バージョンにロールバックします
- `--prefix`を指定した場合は、そのプレフィックスに一致する全てのオブジェクトを並列で処理します

## 開発

### 前提条件

- Go 1.16以上
- AWS認証情報の設定

### テスト

```bash
go test ./...
```
