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

### replayコマンド

replay-listコマンドで生成された変更リストを元に、S3イベントを再現します。

```bash
# 基本的な使用方法
trav replay --source-file changes.json --dest-bucket 宛先バケット名

# 変更元のバケットを指定
trav replay --source-file changes.json --source-bucket 変更元バケット名 --dest-bucket 宛先バケット名

# 再生速度を調整（2倍速）
trav replay --source-file changes.json --dest-bucket 宛先バケット名 --speed-factor 2.0

# 並列処理数を指定
trav replay --source-file changes.json --dest-bucket 宛先バケット名 --concurrency 20

# ドライラン（実際に変更を適用せずに実行）
trav replay --source-file changes.json --dest-bucket 宛先バケット名 --dry-run

# 時間間隔を無視して即時実行
trav replay --source-file changes.json --dest-bucket 宛先バケット名 --ignore-time-windows

# 詳細結果をファイルに出力
trav replay --source-file changes.json --dest-bucket 宛先バケット名 --output result.txt
```

#### オプション

- `-f, --source-file` (必須): 変更リストのファイルパス
- `-b, --dest-bucket` (必須): 変更先のバケット
- `-s, --source-bucket`: 変更元のバケット (指定しない場合は宛先バケットと同じ)
- `-c, --concurrency`: 並列処理数 (デフォルト: 10)
- `-x, --speed-factor`: 再生速度の倍率 (1.0 = 実時間、2.0 = 2倍速) (デフォルト: 1.0)
- `-n, --dry-run`: 実際に変更を適用せずに実行
- `--ignore-time-windows`: 時間間隔を無視して即時実行
- `-o, --output`: 詳細結果の出力ファイルパス
- `-d, --debug`: デバッグモードを有効にする

#### 動作

- イベントは元の時間間隔を保ちながら実行されます
- 同一ファイルへの操作は直列で処理されます
- 異なるファイルへの操作は並列で処理されます
- `--speed-factor`オプションで再生速度を調整できます
- `--dry-run`オプションを指定すると、実際に変更を適用せずに実行できます
- `--ignore-time-windows`オプションを指定すると、時間間隔を無視して即時実行できます

## 開発

### 前提条件

- Go 1.16以上
- AWS認証情報の設定

### テスト

```bash
go test ./...
```
