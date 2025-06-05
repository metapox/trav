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

### replay-listコマンド

指定時間以降のS3オブジェクトの変更を取得し、後でリプレイしやすいフォーマットで出力します。

```bash
# 基本的な使用方法
trav replay-list --bucket バケット名 --timestamp 2023-01-01T12:00:00Z

# プレフィックスを指定
trav replay-list --bucket バケット名 --prefix プレフィックス --timestamp 2023-01-01T12:00:00Z

# ファイルに出力
trav replay-list --bucket バケット名 --timestamp 2023-01-01T12:00:00Z --output changes.json

# 並列処理数とバッチサイズを指定
trav replay-list --bucket バケット名 --timestamp 2023-01-01T12:00:00Z --concurrency 20 --batch-size 5000 --output changes.json

# デバッグモードで実行
trav replay-list --bucket バケット名 --timestamp 2023-01-01T12:00:00Z --debug
```

#### オプション

- `-b, --bucket` (必須): S3バケット名
- `-p, --prefix`: S3オブジェクトのプレフィックス
- `-t, --timestamp` (必須): 取得開始時間 (ISO 8601形式: YYYY-MM-DDThh:mm:ssZ)
- `-o, --output`: 出力ファイルパス (指定しない場合は標準出力)
- `-c, --concurrency`: 並列処理数 (デフォルト: 10)
- `--batch-size`: バッチサイズ (一度に処理するオブジェクト数) (デフォルト: 1000)
- `-d, --debug`: デバッグモードを有効にする

#### 出力フォーマット

JSONフォーマットで、以下の情報が含まれます：

```json
[
  {
    "key": "オブジェクトキー",
    "versionId": "バージョンID",
    "changeType": "CREATE|UPDATE|DELETE|UNDELETE",
    "timestamp": "変更時刻（ISO 8601形式）",
    "size": オブジェクトサイズ（バイト）,
    "etag": "ETag",
    "isDeleteMarker": true|false,
    "previousVersionId": "前のバージョンID（存在する場合）"
  },
  ...
]
```

## 開発

### 前提条件

- Go 1.16以上
- AWS認証情報の設定

### テスト

```bash
go test ./...
```
