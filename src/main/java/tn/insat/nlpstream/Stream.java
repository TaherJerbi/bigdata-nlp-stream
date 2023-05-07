package tn.insat.nlpstream;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class Stream {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("NetworkWordCount")
                .setMaster("local[*]");
        JavaStreamingContext jssc =
                new JavaStreamingContext(conf, Durations.seconds(5));

        JavaReceiverInputDStream<String> lines =
                jssc.socketTextStream("localhost", 9999);

        SentimentAnalyzer sentimentAnalyzer = SentimentAnalyzer.getInstance();

        // analyze sentiment of each line
        JavaDStream<Map<String, Integer>> sentiments = lines.map(sentimentAnalyzer::getSentiment);

        sentiments.print();


        jssc.start();
        jssc.awaitTermination();
    }


}
