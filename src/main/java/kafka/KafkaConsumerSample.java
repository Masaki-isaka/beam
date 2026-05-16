package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Kafkaからメッセージを受信するサンプル。
 *
 * 前提: Kafkaブローカーが localhost:9092 で起動していること。
 * KafkaProducerSample を先に実行してメッセージを送信しておくこと。
 *
 * 実行方法:
 *   mvn compile exec:java -Dexec.mainClass=kafka.KafkaConsumerSample
 *
 * Ctrl+C で停止。
 */
public class KafkaConsumerSample {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC             = "sample-topic";
    private static final String GROUP_ID          = "sample-group";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        // コンシューマーグループID: 同じIDのコンシューマーで負荷分散される
        props.put("group.id", GROUP_ID);
        props.put("key.deserializer",   "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // グループ内に既存のオフセットがない場合、最も古いメッセージから読む
        props.put("auto.offset.reset", "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            System.out.println("メッセージの受信を開始します（Ctrl+C で停止）...");

            while (true) {
                // 最大1秒間ポーリングしてメッセージを取得
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("受信: topic=%s partition=%d offset=%d key=%s value=%s%n",
                            record.topic(), record.partition(), record.offset(),
                            record.key(), record.value());
                }
            }
        }
    }
}
