package tn.insat.nlpstream;

import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreSentence;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SentimentAnalyzer implements Serializable {

    private static SentimentAnalyzer instance = null;
    private Properties props;
    private SentimentAnalyzer(){
        props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment");
    }
    public static SentimentAnalyzer getInstance(){
        if(instance == null){
            instance = new SentimentAnalyzer();
        }
        return instance;
    }

    // add sentiment to each line
    // IN: comment_id, comment_parent_id, comment_body, subreddit, timestamp
    // OUT: comment_id, comment_parent_id, comment_body, subreddit, timestamp, sentiment
    public String addSentiment(String text){
        // create a pipeline inside the function to avoid the error: java.io.NotSerializableException: edu.stanford.nlp.pipeline.StanfordCoreNLP
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        // extract body from the text
        // take from the 2nd comma to the 2nd last comma
        String[] parts = text.split(",");
        String body = String.join(",", Arrays.copyOfRange(parts, 2, parts.length-2));
        System.out.println("Body: "+body);
        // create a document object
        CoreDocument doc = new CoreDocument(text);
        // annotate
        pipeline.annotate(doc);

        HashMap<String, Integer> sentiments = new HashMap<String, Integer>();
        // display sentences
        for (CoreSentence sentence : doc.sentences()) {
            String sentiment = sentence.sentiment();
            System.out.println("    Sentence: "+sentence.toString());
            System.out.println("    Sentiment: "+sentiment);

            if(sentiments.containsKey(sentiment)){
                sentiments.put(sentiment, sentiments.get(sentiment)+1);
            } else {
                sentiments.put(sentiment, 1);
            }
        }

        // get the majority sentiment from the sentiments map
        String majoritySentiment = "";
        int max = 0;
        for(Map.Entry<String, Integer> entry : sentiments.entrySet()){
            if(entry.getValue() > max){
                max = entry.getValue();
                majoritySentiment = entry.getKey();
            }
        }
        System.out.println("Majority Sentiment: "+majoritySentiment);
        return text + "," + majoritySentiment;
    }
}
