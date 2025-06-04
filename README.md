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
# 基本的な使い方（直前のバージョンにロールバック）
trav rollback --bucket バケット名 --key オブジェクトキー

# 特定の時間以前のバージョンにロールバック
trav rollback --bucket バケット名 --key オブジェクトキー --timestamp 2023-01-01T12:00:00Z

# 特定のバージョンにロールバック
trav rollback --bucket バケット名 --key オブジェクトキー --version バージョンID
```

#### オプション

- `-b, --bucket` (必須): S3バケット名
- `-k, --key` (必須): S3オブジェクトキー
- `-t, --timestamp`: ロールバック先の時間 (ISO 8601形式: YYYY-MM-DDThh:mm:ssZ)
- `-v, --version`: ロールバック先の特定バージョンID

タイムスタンプを指定した場合、その時間より前の最新バージョンにロールバックします。
バージョンIDを指定した場合、そのバージョンにロールバックします。
どちらも指定しない場合は、直前のバージョンにロールバックします。

## 開発

### 前提条件

- Go 1.16以上
- AWS認証情報の設定

### テスト

```bash
go test ./...
```
