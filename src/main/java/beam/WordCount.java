package beam;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * Apache Beam ワードカウント サンプル (DirectRunner)
 *
 * <p>
 * 使い方:
 * 
 * <pre>
 *   mvn compile exec:java
 *   # または入力・出力を指定する場合:
 *   mvn compile exec:java -Dexec.args="--inputFile=input.txt --outputFile=output/wordcount"
 * </pre>
 */
public class WordCount {

    /** パイプライン実行オプション */
    public interface WordCountOptions extends PipelineOptions {

        @Description("読み込むテキストファイルのパス")
        @Default.String("input.txt")
        String getInputFile();

        void setInputFile(String value);

        @Description("結果を書き込むファイルのプレフィックス")
        @Default.String("output/wordcount")
        String getOutputFile();

        void setOutputFile(String value);
    }

    public static void main(String[] args) {
        WordCountOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(WordCountOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                // 1. テキストファイルを行単位で読み込む
                .apply("ReadLines", TextIO.read().from(options.getInputFile()))

                // 2. 各行を単語に分割 (英字のみ、小文字化)
                .apply("ExtractWords", FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via(line -> Arrays.asList(line.toLowerCase().split("[^a-zA-Z']+"))))

                // 3. 空文字列を除外
                .apply("FilterEmpty", Filter.by(word -> !word.isEmpty()))

                // 4. 単語ごとに出現回数を集計
                .apply("CountWords", Count.perElement())

                // 5. KV<単語, 件数> を "単語: 件数" 形式の文字列に変換
                .apply("FormatResults", MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Long> kv) -> kv.getKey() + ": " + kv.getValue()))

                // 6. ファイルに書き出す (withNumShards(1) で単一ファイルに統合)
                .apply("WriteResults", TextIO.write().to(options.getOutputFile()).withNumShards(1));

        // パイプラインを実行 (DirectRunner はここでブロック)
        pipeline.run().waitUntilFinish();

        System.out.println("完了: 結果を " + options.getOutputFile() + "-00000-of-00001 に書き込みました");
    }
}
