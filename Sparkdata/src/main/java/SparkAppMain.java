import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;


public class SparkAppMain {

	private static final String HOST = "localhost";
	private static final int PORT = 9999;

	public static void main(String[] args) throws Exception {
	 
	        // SparkConf initialisieren
	        SparkConf conf = new SparkConf()
	                .setMaster("local[*]")
	                .setAppName("VerySimpleStreamingApp");
	        
	        JavaStreamingContext streamingContext =
	                new JavaStreamingContext(conf, Durations.seconds(5));
	        Logger.getRootLogger().setLevel(Level.ERROR);

	        // Empfange die Streaming Daten unseres EventServers über den Localhost Port 9999
	        JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream(HOST, PORT);
	        //lines.print();
	        // Break every message into words and return list of words

	        lines.print();
	        // Starte den streaming context 
	        streamingContext.start();
	        streamingContext.awaitTermination();
	}
}
