package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * Kafkaにメッセージを送信するサンプル。
 *
 * 前提: Kafkaブローカーが localhost:9092 で起動していること。
 * トピック "sample-topic" を事前に作成するか、auto.create.topics.enable=true であること。
 *
 * 実行方法:
 *   mvn compile exec:java -Dexec.mainClass=kafka.KafkaProducerSample
 */
public class KafkaProducerSample {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "sample-topic";

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        // メッセージのキーと値をシリアライズする方法を指定
        props.put("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 全レプリカへの書き込み完了を確認してから成功とみなす（信頼性重視）
        props.put("acks", "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 10; i++) {
                String key   = "key-" + i;
                String value = "Hello Kafka! message=" + i;

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);

                // send() は非同期。get() で完了を待つ（同期送信）
                RecordMetadata meta = producer.send(record).get();

                System.out.printf("送信完了: topic=%s partition=%d offset=%d key=%s value=%s%n",
                        meta.topic(), meta.partition(), meta.offset(), key, value);
            }
        }

        System.out.println("全メッセージの送信が完了しました。");
    }
}
