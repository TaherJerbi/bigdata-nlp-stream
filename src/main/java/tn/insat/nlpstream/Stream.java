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
import java.util.regex.Pattern;

public class Stream {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("NetworkWordCount")
                .setMaster("local[*]");
        JavaStreamingContext jssc =
                new JavaStreamingContext(conf, Durations.seconds(1));

        JavaReceiverInputDStream<String> lines =
                jssc.socketTextStream("localhost", 9999);

        // extract text and subreddit
        JavaPairDStream<String, String> textAndSubreddit = lines.mapToPair(Stream::extractTextAndSubreddit);

        textAndSubreddit.print();

        jssc.start();
        jssc.awaitTermination();
    }


    public static Tuple2<String, String> extractTextAndSubreddit(String line){
        System.out.println("line: " + line);
        try {
            String text = line.replaceAll("\"?,r\\/\\w+", "")
                    .replaceAll("\\d+,\\w+,\\w+,\"?", "");
            String subreddit = line
                    .replaceAll("\\d+,\\w+,\\w+,\"?", "")
                    .replaceAll(text, "");

            return new Tuple2<String, String>(text, subreddit);
        } catch (Exception e){
            return new Tuple2<String, String>("", "");
        }
    }

}
