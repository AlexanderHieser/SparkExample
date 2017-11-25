import java.util.Arrays;


import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Logger;
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

	    
	    String[] filters = {consumerKey,consumerSecret,accessToken,accessTokenSecret};
	    SparkConf sparkConf = new SparkConf().setAppName("SparkTwitterSample");
	    sparkConf.setMaster("local[*]");
	
	    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
	    JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc);
	    stream.print();
	    jssc.start();

	    jssc.awaitTermination();
	   
	}
	
}
