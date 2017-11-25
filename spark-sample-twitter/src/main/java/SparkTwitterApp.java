import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import twitter4j.Status;

public class SparkTwitterApp {

	private final static String consumerKey = "jmrKuCp1Bg0YpTNdOeH4hu7G0";
	private final static String consumerSecret = "vLJob0xGdccZqE6lePitpLaFHDpBSViovylO3754miANNZtPGh";
	private final static String accessToken = "842146459881480193-bIPVULAmaHOQ7wK6cRqZalsvAyre2y0";
	private final static String accessTokenSecret = "DFEeXXEMIR9aAzAwz8kNr4Swmh86BX06etpMgD7lj1OOC";

	public static void main(String[] args) {

		System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
		System.setProperty("twitter4j.oauth.accessToken", accessToken);
		System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

		
		SparkConf sparkConf = new SparkConf().setAppName("SparkTwitterSample");
		sparkConf.setMaster("local[*]");

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
		JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc);
		
		//StreamingExamples.setStreamingLogLevels();
	    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)
	    if (!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
	      Logger.getRootLogger().setLevel(Level.WARN);
	    }

	    JavaDStream<String> words = stream.flatMap(new FlatMapFunction<Status, String>() {
			public Iterable<String> call(Status arg0) throws Exception {
			      return Arrays.asList(arg0.getText().split(" "));
			}
	      });

	    JavaDStream<String> hashTags = words.filter(new Function<String, Boolean>() {

			public Boolean call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0.startsWith("#");
			};
	    	  
	      });
	    

	    // Read in the word-sentiment list and create a static RDD from it
	    String wordSentimentFilePath = "streaming-twitter/examples/data/AFINN-111.txt";
	    final JavaPairRDD<String, Double> wordSentiments = jssc.sparkContext()
	      .textFile(wordSentimentFilePath)
	      .mapToPair(new PairFunction<String, String, Double>(){
	        @Override
	        public Tuple2<String, Double> call(String line) {
	          String[] columns = line.split("\t");
	          return new Tuple2<>(columns[0], Double.parseDouble(columns[1]));
	        }
	      });

	    JavaPairDStream<String, Integer> hashTagCount = hashTags.mapToPair(
	      new PairFunction<String, String, Integer>() {
	        @Override
	        public Tuple2<String, Integer> call(String s) {
	          // leave out the # character
	          return new Tuple2<>(s.substring(1), 1);
	        }
	      });

	    JavaPairDStream<String, Integer> hashTagTotals = hashTagCount.reduceByKeyAndWindow(
	      new Function2<Integer, Integer, Integer>() {
	        @Override
	        public Integer call(Integer a, Integer b) {
	          return a + b;
	        }
	      }, new Duration(10000));

	    // Determine the hash tags with the highest sentiment values by joining the streaming RDD
	    // with the static RDD inside the transform() method and then multiplying
	    // the frequency of the hash tag by its sentiment value
	    JavaPairDStream<String, Tuple2<Double, Integer>> joinedTuples =
	      hashTagTotals.transformToPair(new Function<JavaPairRDD<String, Integer>,
	        JavaPairRDD<String, Tuple2<Double, Integer>>>() {
	        @Override
	        public JavaPairRDD<String, Tuple2<Double, Integer>> call(
	            JavaPairRDD<String, Integer> topicCount) {
	          return wordSentiments.join(topicCount);
	        }
	      });

	    JavaPairDStream<String, Double> topicHappiness = joinedTuples.mapToPair(
	      new PairFunction<Tuple2<String, Tuple2<Double, Integer>>, String, Double>() {
	        @Override
	        public Tuple2<String, Double> call(Tuple2<String,
	          Tuple2<Double, Integer>> topicAndTuplePair) {
	          Tuple2<Double, Integer> happinessAndCount = topicAndTuplePair._2();
	          return new Tuple2<>(topicAndTuplePair._1(),
	            happinessAndCount._1() * happinessAndCount._2());
	        }
	      });

	    JavaPairDStream<Double, String> happinessTopicPairs = topicHappiness.mapToPair(
	      new PairFunction<Tuple2<String, Double>, Double, String>() {
	        @Override
	        public Tuple2<Double, String> call(Tuple2<String, Double> topicHappiness) {
	          return new Tuple2<>(topicHappiness._2(),
	            topicHappiness._1());
	        }
	      });

	    JavaPairDStream<Double, String> happiest10 = happinessTopicPairs.transformToPair(
	      new Function<JavaPairRDD<Double, String>, JavaPairRDD<Double, String>>() {
	        @Override
	        public JavaPairRDD<Double, String> call(
	            JavaPairRDD<Double, String> happinessAndTopics) {
	          return happinessAndTopics.sortByKey(false);
	        }
	      }
	    );

	    // Print hash tags with the most positive sentiment values
	    happiest10.foreachRDD(new VoidFunction<JavaPairRDD<Double, String>>() {
	      @Override
	      public void call(JavaPairRDD<Double, String> happinessTopicPairs) {
	        List<Tuple2<Double, String>> topList = happinessTopicPairs.take(10);
	        System.out.println(
	          String.format("\nHappiest topics in last 10 seconds (%s total):",
	            happinessTopicPairs.count()));
	        for (Tuple2<Double, String> pair : topList) {
	          System.out.println(
	            String.format("%s (%s happiness)", pair._2(), pair._1()));
	        }
	      }
	    });
	    
		stream.print();
		jssc.start();

		jssc.awaitTermination();

	}

}
