# Apache Beam と Apache Kafka 基礎まとめ

---

## 目次

1. [Apache Kafka](#1-apache-kafka)
   - [概要](#11-概要)
   - [主要コンセプト](#12-主要コンセプト)
   - [メッセージの流れ](#13-メッセージの流れ)
   - [コンシューマーグループ](#14-コンシューマーグループ)
   - [オフセット管理](#15-オフセット管理)
2. [Apache Beam](#2-apache-beam)
   - [概要](#21-概要)
   - [主要コンセプト](#22-主要コンセプト)
   - [Runner](#23-runner)
   - [ウィンドウ処理](#24-ウィンドウ処理)
   - [トリガー](#25-トリガー)
3. [Beam と Kafka の連携](#3-beam-と-kafka-の連携)
   - [役割分担](#31-役割分担)
   - [KafkaIO](#32-kafkaio)
   - [このプロジェクトのアーキテクチャ](#33-このプロジェクトのアーキテクチャ)
4. [用語早見表](#4-用語早見表)

---

## 1. Apache Kafka

### 1.1 概要

Kafka は **分散イベントストリーミングプラットフォーム**。  
アプリケーション間のメッセージを高スループット・低レイテンシで中継する「パイプ」として機能する。

```
Producer  ──→  [ Kafka クラスター ]  ──→  Consumer
               (メッセージを保持)
```

**特徴:**

| 特徴 | 説明 |
|------|------|
| 永続化 | メッセージをディスクに保存。Consumer が遅れても取りこぼさない |
| スケーラブル | Partition を増やして水平スケールアウト |
| 再生可能 | オフセットを指定して過去のメッセージを再読み込みできる |
| 疎結合 | Producer と Consumer が互いを知らなくてよい |

---

### 1.2 主要コンセプト

#### Broker
Kafka サーバー本体。メッセージの受信・保存・配信を担う。  
本番環境では複数台をクラスター構成にして冗長化する。

#### Topic
メッセージの**論理的な分類単位**。フォルダのようなもの。

```
Topic: "beam-input"   ← TextMessageProducer が書き込む
Topic: "beam-output"  ← KafkaWordCountPipeline が書き込む
```

#### Partition
Topic を分割した**物理的な単位**。並列処理の基本単位。

```
Topic: "beam-input"
  ├── Partition 0: [msg0][msg1][msg2]...
  ├── Partition 1: [msg3][msg4][msg5]...
  └── Partition 2: [msg6][msg7][msg8]...
```

- Partition が多いほど並列読み書きが可能
- 同一 Partition 内のメッセージは**順序が保証**される

#### Producer
Kafka へメッセージを**書き込む**側。  
どの Topic・Partition に書くかを指定する。

#### Consumer
Kafka からメッセージを**読み取る**側。  
Partition を自分でポーリングして取得する（プッシュではなくプル型）。

#### Record（メッセージ）
Kafka の 1 件のメッセージ。以下の要素で構成される。

| フィールド | 説明 | 例 |
|-----------|------|-----|
| Key | ルーティングや識別に使う | `"msg-0"` |
| Value | 本体データ | `"Hello Apache Beam"` |
| Timestamp | 書き込み時刻 | `1716000000000` |
| Offset | Partition 内の連番 | `10` |

---

### 1.3 メッセージの流れ

```
Producer
  │  send("beam-input", key="msg-0", value="Hello Beam")
  ▼
Kafka Broker
  │  Partition 0 に追記: offset=10
  ▼
Consumer
  │  poll() → ConsumerRecord(offset=10, key="msg-0", value="Hello Beam")
  ▼
アプリケーションの処理
```

---

### 1.4 コンシューマーグループ

同じ `group.id` を持つ Consumer の集まり。  
**1 つの Partition は 1 つの Consumer にしか割り当てられない**ため、自動的に負荷分散される。

```
Topic (3 Partitions)        Consumer Group "my-group"
  Partition 0  ──────────→  Consumer A
  Partition 1  ──────────→  Consumer B
  Partition 2  ──────────→  Consumer C
```

異なるグループは**独立してオフセットを管理**するため、同じメッセージを別々に処理できる。

---

### 1.5 オフセット管理

Consumer は「どこまで読んだか」をオフセットで管理する。

| 設定値 | 動作 |
|--------|------|
| `auto.offset.reset=earliest` | 最初から読む（過去のメッセージも取得） |
| `auto.offset.reset=latest` | 起動後の新着メッセージのみ読む |

オフセットはコミットにより Kafka に保存され、再起動後も続きから読める。

---

## 2. Apache Beam

### 2.1 概要

Beam は**データ処理パイプラインの統一モデル**を提供する SDK。  
「どう処理するか」を記述するだけで、実行エンジン（Runner）を差し替えられる。

```
Beam SDK でパイプラインを記述
        ↓
  Runner を選んで実行
  ┌─────────────────────┐
  │ DirectRunner  (開発) │
  │ Dataflow      (GCP)  │
  │ Flink         (OSS)  │
  │ Spark         (OSS)  │
  └─────────────────────┘
```

**バッチとストリーミングの両方**を同じコードで扱えるのが最大の特徴。

---

### 2.2 主要コンセプト

#### Pipeline
処理全体を表すオブジェクト。DAG（有向非巡回グラフ）として構成される。

```java
Pipeline pipeline = Pipeline.create(options);
```

#### PCollection
パイプライン内を流れる**データのコレクション**。

| 種類 | 特徴 |
|------|------|
| 有界 (Bounded) | ファイルや DB など終端のあるデータ。バッチ処理向き |
| 無界 (Unbounded) | Kafka や IoT など終端のないデータ。ストリーミング向き |

#### PTransform
PCollection を別の PCollection に変換する**処理の単位**。

```
PCollection<String>
  .apply("ExtractWords", FlatMapElements...)   // 1行 → 複数単語
  .apply("FilterEmpty",  Filter.by(...))       // 空文字除外
  .apply("CountWords",   Count.perElement())   // 単語ごとにカウント
```

よく使う組み込み Transform:

| Transform | 用途 |
|-----------|------|
| `MapElements` | 1要素 → 1要素の変換 |
| `FlatMapElements` | 1要素 → 複数要素の変換 |
| `Filter` | 条件に合う要素だけ残す |
| `Count.perElement()` | 要素ごとの出現回数を集計 |
| `GroupByKey` | Key でグループ化 |
| `Combine` | 集約（sum, mean など） |

#### KV（Key-Value）
Beam でよく使われるペア型。Kafka のレコードと自然に対応する。

```java
KV<String, Long> kv = KV.of("apache", 5L);
kv.getKey();   // "apache"
kv.getValue(); // 5L
```

---

### 2.3 Runner

Beam のコードを実際に**実行するエンジン**。コードを変えずに切り替えられる。

| Runner | 特徴 | 用途 |
|--------|------|------|
| DirectRunner | ローカルのシングルプロセスで動作 | 開発・テスト |
| Google Dataflow | フルマネージド、自動スケーリング | GCP 本番環境 |
| Apache Flink | OSS の分散処理エンジン | オンプレ本番環境 |
| Apache Spark | バッチ処理が得意な分散エンジン | バッチ中心の環境 |

Runner の指定方法:

```bash
# DirectRunner（デフォルト）
mvn exec:java -Dexec.mainClass=beam.MyPipeline

# Dataflow Runner
mvn exec:java -Dexec.mainClass=beam.MyPipeline \
  -Dexec.args="--runner=DataflowRunner --project=my-gcp-project"
```

---

### 2.4 ウィンドウ処理

無界ストリームを**有限の塊**に区切って集計する仕組み。  
ウィンドウなしでは、終わらないストリームを集計できない。

#### Fixed Window（固定ウィンドウ）
一定時間ごとに区切る。重複なし。

```
時間軸: 0----5----10----15----20 (秒)
         [---- W1 ----][---- W2 ----]
```

```java
Window.into(FixedWindows.of(Duration.standardSeconds(10)))
```

#### Sliding Window（スライディングウィンドウ）
一定間隔でずれながら重複して区切る。移動平均などに向く。

```
時間軸: 0----5----10----15----20
         [------ W1 ------]
              [------ W2 ------]
                   [------ W3 ------]
```

#### Session Window（セッションウィンドウ）
一定時間メッセージが来なければウィンドウを閉じる。ユーザー行動分析などに向く。

```
メッセージ: * * *      * *         * *
                  ↑gap↑           ↑gap↑
            [-- W1 --]  [-- W2 --]  [W3]
```

---

### 2.5 トリガー

ウィンドウの結果を**いつ出力するか**を制御する仕組み。

| トリガー | タイミング |
|---------|-----------|
| `AfterWatermark` | ウォーターマーク到達時（デフォルト） |
| `AfterProcessingTime` | 処理時間の経過後 |
| `AfterCount` | 指定件数に達したとき |
| `Repeatedly` | 繰り返し発火 |

**ウォーターマーク**: 「このタイムスタンプより前のデータはもう来ない」という目安。  
遅延データの扱いを制御するために使われる。

---

## 3. Beam と Kafka の連携

### 3.1 役割分担

```
┌─────────────────────────────────────────────────────┐
│  Kafka                                              │
│  ・メッセージの輸送・保持                           │
│  ・Producer/Consumer 間の疎結合                     │
│  ・オフセットによる再生・耐障害性                   │
└─────────────────────────────────────────────────────┘
           ↕  KafkaIO（Beam の I/O コネクタ）
┌─────────────────────────────────────────────────────┐
│  Apache Beam                                        │
│  ・データの変換・集計ロジック                       │
│  ・ウィンドウ処理・ストリーミング制御               │
│  ・Runner 切り替えによるポータビリティ              │
└─────────────────────────────────────────────────────┘
```

Kafka は「どう運ぶか」、Beam は「どう処理するか」を担う。

---

### 3.2 KafkaIO

Beam が Kafka を読み書きするための組み込みコネクタ。

#### KafkaIO.read()

```java
KafkaIO.<String, String>read()
    .withBootstrapServers("localhost:9092")  // Kafka ブローカーのアドレス
    .withTopic("beam-input")                 // 読み込むトピック
    .withKeyDeserializer(StringDeserializer.class)
    .withValueDeserializer(StringDeserializer.class)
    .withoutMetadata()  // KafkaRecord → KV<K,V> にシンプル化
```

- Kafka トピックを**無界ソース**として扱う
- パイプラインが自動的にストリーミングモードで起動する
- `withoutMetadata()` を付けると `KV<K, V>` として後続の Transform に渡せる

#### KafkaIO.write()

```java
KafkaIO.<String, String>write()
    .withBootstrapServers("localhost:9092")
    .withTopic("beam-output")
    .withKeySerializer(StringSerializer.class)
    .withValueSerializer(StringSerializer.class)
```

- 上流の `PCollection<KV<K, V>>` を Kafka トピックに書き込む

---

### 3.3 このプロジェクトのアーキテクチャ

```
[TextMessageProducer]
  │  10 件のサンプル文を送信
  ▼
[Kafka: beam-input トピック]
  │  KafkaIO.read()（無界ソース）
  ▼
[KafkaWordCountPipeline]
  │
  ├─ FlatMapElements  : 文 → 単語リスト
  ├─ Filter           : 空文字除外
  ├─ FixedWindows(10s): 10 秒ごとにウィンドウを区切る
  ├─ Count.perElement : ウィンドウ内で単語をカウント
  └─ MapElements      : KV<word, count> → KV<word, "word: N">
  │  KafkaIO.write()
  ▼
[Kafka: beam-output トピック]
  │
  ├─ [KafkaConsumerSample]  : シンプルな受信確認
  └─ [BeamOutputConsumer]   : ウィンドウ単位でランキング表示
```

**実行手順:**

```bash
# 1. Kafka 起動
docker compose up -d

# 2. パイプライン起動（Ctrl+C まで待機）
mvn compile exec:java -Dexec.mainClass=beam.KafkaWordCountPipeline

# 3. メッセージ投入（別ターミナル）
mvn compile exec:java -Dexec.mainClass=kafka.TextMessageProducer

# 4. 結果確認（別ターミナル）
mvn compile exec:java -Dexec.mainClass=kafka.BeamOutputConsumer
```

---

## 4. 用語早見表

| 用語 | 所属 | 一言説明 |
|------|------|---------|
| Broker | Kafka | Kafka サーバー本体 |
| Topic | Kafka | メッセージの分類単位（フォルダ的存在） |
| Partition | Kafka | Topic の物理分割。並列処理の単位 |
| Offset | Kafka | Partition 内のメッセージ連番 |
| Producer | Kafka | Kafka へ書き込む側 |
| Consumer | Kafka | Kafka から読み取る側 |
| Consumer Group | Kafka | 同じ group.id を持つ Consumer の集まり |
| Pipeline | Beam | 処理全体を表す DAG |
| PCollection | Beam | パイプライン内を流れるデータのコレクション |
| PTransform | Beam | PCollection を変換する処理の単位 |
| Runner | Beam | パイプラインの実行エンジン（Flink, Dataflow など） |
| Bounded | Beam | 終端のあるデータ（バッチ） |
| Unbounded | Beam | 終端のないデータ（ストリーミング） |
| Fixed Window | Beam | 固定時間で区切るウィンドウ |
| Watermark | Beam | 「これ以前のデータは来ない」という時刻の目安 |
| KafkaIO | Beam | Beam が Kafka を読み書きする I/O コネクタ |
| KV | Beam | Key-Value ペアの型 |
