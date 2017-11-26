import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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
		      Logger.getRootLogger().setLevel(Level.OFF);
	    

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
	    
	    hashTagCount.print(100);
	    hashTagTotals.print(100);
		jssc.start();

		jssc.awaitTermination();

	}

}
