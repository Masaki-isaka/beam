package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;

/**
 * {@link beam.KafkaWordCountPipeline} の処理結果を受信・表示するコンシューマー。
 *
 * <h2>受信するメッセージの形式</h2>
 * <ul>
 *   <li>トピック: {@code beam-output}</li>
 *   <li>Key  : 単語（例: {@code "apache"}）</li>
 *   <li>Value: "単語: カウント数"（例: {@code "apache: 3"}）</li>
 * </ul>
 *
 * <p>固定ウィンドウ（10 秒）ごとにまとめて届くため、
 * このコンシューマーは同一ウィンドウのレコードを集めて
 * ランキング形式で表示する。
 *
 * <h2>実行手順</h2>
 * <ol>
 *   <li>Kafka を起動する:
 *       <pre>docker compose up -d</pre></li>
 *   <li>{@link beam.KafkaWordCountPipeline} を起動する:
 *       <pre>mvn compile exec:java -Dexec.mainClass=beam.KafkaWordCountPipeline</pre></li>
 *   <li>このコンシューマーを起動する（結果を待ち受ける）:
 *       <pre>mvn compile exec:java -Dexec.mainClass=kafka.BeamOutputConsumer</pre></li>
 *   <li>別ターミナルからメッセージを投入する:
 *       <pre>mvn compile exec:java -Dexec.mainClass=kafka.TextMessageProducer</pre></li>
 * </ol>
 *
 * @see beam.KafkaWordCountPipeline
 * @see TextMessageProducer
 */
public class BeamOutputConsumer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    /** KafkaWordCountPipeline が結果を書き出すトピック。 */
    private static final String TOPIC = "beam-output";

    private static final String GROUP_ID = "beam-output-viewer";

    /** ウィンドウ結果の区切りと判定するポーリング無応答回数の閾値。 */
    private static final int FLUSH_AFTER_EMPTY_POLLS = 3;

    private static final DateTimeFormatter TIME_FMT =
            DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", GROUP_ID);
        props.put("key.deserializer",   "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // パイプライン起動前に送られたメッセージも確認できるよう earliest から読む
        props.put("auto.offset.reset", "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));

            System.out.println("═".repeat(60));
            System.out.println("  Beam ワードカウント結果コンシューマー 起動");
            System.out.println("  トピック : " + TOPIC);
            System.out.println("  KafkaWordCountPipeline からの結果を待ち受けています...");
            System.out.println("  Ctrl+C で停止");
            System.out.println("═".repeat(60));

            // 同一ウィンドウ内のレコードをまとめて保持するバッファ
            // TreeMap を使い単語を辞書順に並べる
            TreeMap<String, Integer> windowBuffer = new TreeMap<>();
            int emptyPollCount = 0;

            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofSeconds(1));

                if (records.isEmpty()) {
                    emptyPollCount++;

                    // 一定回数応答がなければウィンドウの区切りとみなして集計表示
                    if (!windowBuffer.isEmpty() && emptyPollCount >= FLUSH_AFTER_EMPTY_POLLS) {
                        printWindowSummary(windowBuffer);
                        windowBuffer.clear();
                        emptyPollCount = 0;
                    }
                    continue;
                }

                emptyPollCount = 0;

                for (ConsumerRecord<String, String> record : records) {
                    String word  = record.key();
                    String value = record.value(); // "word: N" 形式

                    // value から カウント数を抽出してバッファに追記
                    int count = parseCount(value);
                    windowBuffer.merge(word, count, (a, b) -> a + b);

                    System.out.printf("[%s] 受信  key=%-15s  value=%s%n",
                            TIME_FMT.format(Instant.now()), word, value);
                }
            }
        }
    }

    /**
     * ウィンドウ内の集計結果を降順ランキング形式で表示する。
     *
     * @param buffer 単語とカウント数のマップ
     */
    private static void printWindowSummary(TreeMap<String, Integer> buffer) {
        System.out.println();
        System.out.println("┌─────────────────────────────────────────┐");
        System.out.printf( "│  ウィンドウ集計結果 (%s)          │%n",
                TIME_FMT.format(Instant.now()));
        System.out.println("├──────────────────────┬──────────────────┤");
        System.out.println("│  単語                │  カウント        │");
        System.out.println("├──────────────────────┼──────────────────┤");

        // カウント数の降順でソートして表示
        buffer.entrySet().stream()
              .sorted((a, b) -> b.getValue() - a.getValue())
              .forEach(e -> System.out.printf("│  %-20s│  %-16d│%n",
                      e.getKey(), e.getValue()));

        System.out.println("└──────────────────────┴──────────────────┘");
        System.out.println();
    }

    /**
     * "word: N" 形式の文字列からカウント数を取り出す。
     *
     * @param value KafkaWordCountPipeline が書き込んだ value 文字列
     * @return カウント数。パース失敗時は 0
     */
    private static int parseCount(String value) {
        try {
            String[] parts = value.split(": ");
            return Integer.parseInt(parts[parts.length - 1].trim());
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            return 0;
        }
    }
}
