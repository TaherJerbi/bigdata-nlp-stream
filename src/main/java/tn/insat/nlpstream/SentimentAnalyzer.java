package tn.insat.nlpstream;

import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreSentence;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SentimentAnalyzer implements Serializable {

    private static SentimentAnalyzer instance = null;
    private Properties props;
    private StanfordCoreNLP pipeline;
    private SentimentAnalyzer(){
        props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment");
        pipeline = new StanfordCoreNLP(props);
    }
    public static SentimentAnalyzer getInstance(){
        if(instance == null){
            instance = new SentimentAnalyzer();
        }
        return instance;
    }

    public Map<String, Integer> getSentiment(String text){
        // create a document object
        CoreDocument doc = new CoreDocument(text);
        // annotate
        pipeline.annotate(doc);

        HashMap<String, Integer> sentiments = new HashMap<String, Integer>();
        // display sentences
        for (CoreSentence sentence : doc.sentences()) {
            String sentiment = sentence.sentiment();
            System.out.println("Sentence: "+sentence.toString());
            System.out.println("Sentiment: "+sentiment);

            if(sentiments.containsKey(sentiment)){
                sentiments.put(sentiment, sentiments.get(sentiment)+1);
            } else {
                sentiments.put(sentiment, 1);
            }
        }
        return sentiments;
    }
}
