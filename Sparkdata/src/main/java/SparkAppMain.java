import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkAppMain {

	   private static final String HOST = "localhost";
	    private static final int PORT = 9999;

	public static void main(String[] args) throws Exception {
	 

	        // Configure and initialize the SparkStreamingContext
	        SparkConf conf = new SparkConf()
	                .setMaster("local[*]")
	                .setAppName("VerySimpleStreamingApp");
	        
	        JavaStreamingContext streamingContext =
	                new JavaStreamingContext(conf, Durations.seconds(5));
	        Logger.getRootLogger().setLevel(Level.ERROR);

	        // Receive streaming data from the source
	        JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream(HOST, PORT);
	        lines.print();

	        // Execute the Spark workflow defined above
	        streamingContext.start();
	        streamingContext.awaitTermination();
	}
}