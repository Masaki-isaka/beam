package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * KafkaWordCountPipeline のテスト用プロデューサー。
 * "beam-input" トピックに英語の文章を送信する。
 *
 * 実行方法:
 *   mvn compile exec:java -Dexec.mainClass=kafka.TextMessageProducer
 *
 * 前提: KafkaWordCountPipeline が先に起動していること。
 */
public class TextMessageProducer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "beam-input";

    // Beam パイプラインによる単語カウントが確認しやすいよう、
    // 同じ単語が複数回登場する文章を用意している
    private static final String[] MESSAGES = {
        "Apache Beam is a unified model for batch and streaming data processing",
        "Apache Kafka is a distributed event streaming platform",
        "Beam and Kafka work together for real time data pipelines",
        "Beam reads from Kafka using KafkaIO and writes back to Kafka",
        "Streaming data processing with Apache Beam and Apache Kafka",
        "Fixed windows in Beam group streaming events by time",
        "Apache Beam supports both batch and streaming pipelines",
        "Kafka topics carry messages between producers and consumers",
        "Beam transforms data using PTransforms applied to PCollections",
        "Real time word count with Beam and Kafka is easy to implement",
    };

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");

        System.out.printf("トピック '%s' へ %d 件のメッセージを送信します...%n",
                TOPIC, MESSAGES.length);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < MESSAGES.length; i++) {
                String key   = "msg-" + i;
                String value = MESSAGES[i];

                RecordMetadata meta = producer.send(new ProducerRecord<>(TOPIC, key, value)).get();

                System.out.printf("送信: partition=%d offset=%d | %s%n",
                        meta.partition(), meta.offset(), value);
            }
        }

        System.out.println("\n送信完了。KafkaWordCountPipeline の beam-output トピックに結果が現れます。");
        System.out.println("確認: KafkaConsumerSample の TOPIC を \"beam-output\" に変えて実行してください。");
    }
}
