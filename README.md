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

S3オブジェクトを以前のバージョンにロールバックします。

```bash
# 基本的な使い方
trav rollback --bucket バケット名 --key オブジェクトキー

# 特定のバージョンにロールバック
trav rollback --bucket バケット名 --key オブジェクトキー --version バージョンID
```

#### オプション

- `-b, --bucket` (必須): S3バケット名
- `-k, --key` (必須): S3オブジェクトキー
- `-v, --version`: ロールバック先のバージョンID (指定がない場合は直前のバージョン)

## 開発

### 前提条件

- Go 1.16以上
- AWS認証情報の設定

### テスト

```bash
go test ./...
```
