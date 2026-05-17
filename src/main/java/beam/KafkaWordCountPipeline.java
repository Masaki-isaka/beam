package beam;

import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;

/**
 * Kafka と Apache Beam を組み合わせたストリーミング ワードカウント パイプライン。
 *
 * <h2>アーキテクチャ概要</h2>
 * <pre>
 *   [Kafka トピック: beam-input]
 *         ↓  KafkaIO.read()  ← 無境界ソース。新着メッセージを常時監視する。
 *   ┌─────────────────────────────────────────────────────┐
 *   │  Apache Beam パイプライン (Ctrl+C まで常駐)         │
 *   │  ① 単語に分割        FlatMapElements               │
 *   │  ② 10 秒ウィンドウ   Window.into(FixedWindows)     │
 *   │  ③ 単語ごとカウント  Count.perElement              │
 *   │  ④ 文字列に整形      MapElements                   │
 *   └─────────────────────────────────────────────────────┘
 *         ↓  KafkaIO.write() ← 処理結果を出力トピックへ書き込む。
 *   [Kafka トピック: beam-output]
 * </pre>
 *
 * <h2>Kafka と Beam の役割分担</h2>
 * <ul>
 *   <li><b>Kafka</b>: メッセージの輸送層。プロデューサーがいつでも送信でき、
 *       Beam がそれを拾い上げる「キュー」として機能する。</li>
 *   <li><b>KafkaIO.read()</b>: Kafka トピックを「無境界ソース (Unbounded Source)」として
 *       扱い、パイプラインをストリーミングモードで動作させる。</li>
 *   <li><b>Beam transforms</b>: ウィンドウ集計など、分散処理のビジネスロジックを担う。</li>
 *   <li><b>KafkaIO.write()</b>: 処理結果を別トピックへ書き戻す。</li>
 * </ul>
 *
 * <h2>実行手順</h2>
 * <ol>
 *   <li>Kafka を起動する:
 *       <pre>docker compose up -d</pre></li>
 *   <li>このパイプラインを起動する (Ctrl+C まで待機し続ける):
 *       <pre>mvn compile exec:java -Dexec.mainClass=beam.KafkaWordCountPipeline</pre></li>
 *   <li>別ターミナルからテストメッセージを投入する:
 *       <pre>mvn compile exec:java -Dexec.mainClass=kafka.TextMessageProducer</pre></li>
 *   <li>10 秒後に結果を確認する
 *       ({@code KafkaConsumerSample} の {@code TOPIC} を {@code "beam-output"} に変えて実行):
 *       <pre>mvn compile exec:java -Dexec.mainClass=kafka.KafkaConsumerSample</pre></li>
 * </ol>
 *
 * @see KafkaIO
 * @see kafka.TextMessageProducer
 * @see kafka.KafkaConsumerSample
 */
public class KafkaWordCountPipeline {

    /** Kafka ブローカーのアドレス。 */
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    /** 入力トピック。{@link kafka.TextMessageProducer} がメッセージを書き込む先。 */
    private static final String INPUT_TOPIC = "beam-input";

    /** 出力トピック。Beam による集計結果が書き込まれる先。 */
    private static final String OUTPUT_TOPIC = "beam-output";

    /**
     * 固定ウィンドウのサイズ（秒）。
     * この時間内に届いたメッセージをまとめて単語カウントする。
     */
    private static final int WINDOW_SIZE_SECONDS = 10;

    /**
     * パイプラインを構築して実行する。
     *
     * <p>KafkaIO.read() は無境界ソースのため、パイプラインは自動的にストリーミングモードで起動する。
     * {@code pipeline.run().waitUntilFinish()} がブロックし続けることで、
     * Ctrl+C が押されるまで Kafka からのメッセージ受信と処理を継続する。
     *
     * @param args コマンドライン引数（Beam の PipelineOptions として解釈される）
     */
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

        Pipeline pipeline = Pipeline.create(options);

        pipeline
            // ------------------------------------------------------------------
            // Step 1: Kafka トピックからメッセージを読み込む
            //
            // KafkaIO.read() は「無境界ソース」として動作する。
            // 無境界ソースとは終端のないデータストリームで、パイプラインは
            // 自動的にストリーミングモードに切り替わる。
            //
            // .withoutMetadata() により、KafkaRecord<K,V> ではなく
            // シンプルな KV<K,V> として後続の transform に渡される。
            // ------------------------------------------------------------------
            .apply("ReadFromKafka", KafkaIO.<String, String>read()
                .withBootstrapServers(BOOTSTRAP_SERVERS)
                .withTopic(INPUT_TOPIC)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withoutMetadata())

            // ------------------------------------------------------------------
            // Step 2: メッセージの value（文章）を単語のリストに分割する
            //
            // 例: KV<"msg-0", "Hello Apache Beam">
            //      → ["hello", "apache", "beam"]
            //
            // value のみを使い、アルファベット以外の文字で分割する。
            // ------------------------------------------------------------------
            .apply("ExtractWords", FlatMapElements
                .into(TypeDescriptors.strings())
                .via((KV<String, String> kv) ->
                    Arrays.asList(kv.getValue().toLowerCase().split("[^a-zA-Z']+"))))

            // ------------------------------------------------------------------
            // Step 3: 分割結果から空文字列を除外する
            //
            // 文頭・文末や連続した区切り文字から生じる空要素を取り除く。
            // ------------------------------------------------------------------
            .apply("FilterEmpty", Filter.by(word -> !word.isEmpty()))

            // ------------------------------------------------------------------
            // Step 4: 固定ウィンドウ（Fixed Window）を適用する
            //
            // 無境界のストリームを一定時間ごとのバッチに区切り、
            // 各バッチ内で集計できるようにする。
            //
            // 例（WINDOW_SIZE_SECONDS = 10 の場合）:
            //   |--- 0〜10秒 ---|--- 10〜20秒 ---|--- 20〜30秒 ---|  ...
            //        ↑ この単位ごとに Count.perElement() が集計される
            // ------------------------------------------------------------------
            .apply("ApplyFixedWindow", Window.into(
                FixedWindows.of(Duration.standardSeconds(WINDOW_SIZE_SECONDS))))

            // ------------------------------------------------------------------
            // Step 5: ウィンドウ内で単語ごとに出現回数をカウントする
            //
            // 出力例: KV<"apache", 3L>
            // ------------------------------------------------------------------
            .apply("CountWords", Count.perElement())

            // ------------------------------------------------------------------
            // Step 6: KafkaIO.write() が受け付ける KV<String, String> 形式に整形する
            //
            // KafkaIO.write() のキーと値はどちらも String 型が必要なため、
            // Long 型のカウントを文字列に変換する。
            //
            // 例: KV<"apache", 3L> → KV<"apache", "apache: 3">
            // ------------------------------------------------------------------
            .apply("FormatAsKV", MapElements
                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via((KV<String, Long> kv) ->
                    KV.of(kv.getKey(), kv.getKey() + ": " + kv.getValue())))

            // ------------------------------------------------------------------
            // Step 7: 集計結果を Kafka の出力トピックへ書き込む
            //
            // Kafka レコードの構成:
            //   Key   = 単語（例: "apache"）
            //   Value = "単語: カウント数"（例: "apache: 3"）
            // ------------------------------------------------------------------
            .apply("WriteToKafka", KafkaIO.<String, String>write()
                .withBootstrapServers(BOOTSTRAP_SERVERS)
                .withTopic(OUTPUT_TOPIC)
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class));

        // ストリーミングパイプラインはここでブロックし、Ctrl+C が押されるまで動き続ける。
        pipeline.run().waitUntilFinish();
    }
}
