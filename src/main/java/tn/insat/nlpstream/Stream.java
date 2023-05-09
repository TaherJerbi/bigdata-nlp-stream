package tn.insat.nlpstream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Stream {
    public static void main(String[] args) throws InterruptedException {
        if (args.length < 4) {
            System.err.println("Usage: SparkKafkaWordCount <zkQuorum> <group> <in-topics> <numThreads> <out-topic>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf()
                .setAppName("SparkSentimentAnalysis")
                .setMaster("local[*]");
        JavaStreamingContext jssc =
                new JavaStreamingContext(conf, Durations.seconds(5));

        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<>();
        String[] topics = args[2].split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, args[0], args[1], topicMap);

        JavaDStream<String> lines = messages.map(Tuple2::_2);


        SentimentAnalyzer sentimentAnalyzer = SentimentAnalyzer.getInstance();

        // analyze the majority sentiment of each line and add it to the line
        JavaDStream<String> comments_sentiments = lines.map(sentimentAnalyzer::addSentiment);


        // only send if args[4] is not empty
        if (args.length < 5) {
            comments_sentiments.print();
            jssc.start();
            jssc.awaitTermination();
            return;
        }
        String outputTopic = args[4];
        JavaPairDStream<String, String> output = comments_sentiments.mapToPair(line -> new Tuple2<>("key", line));
        output.foreachRDD(rdd -> {
            rdd.foreachPartition(partition -> {
                KafkaProducer<String, String> producer = createKafkaProducer();
                while (partition.hasNext()) {
                    Tuple2<String, String> record = partition.next();
                    String value = record._2();
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(outputTopic, value);
                    producer.send(producerRecord);
                }
                producer.close();
            });
        });

        comments_sentiments.print();
        jssc.start();
        jssc.awaitTermination();
    }

    private static KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }


}
