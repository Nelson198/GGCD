package stream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Trending
 * TODO - Acabar
 */
public class Trending {
    public static void main(String[] args) throws InterruptedException {
        long time = System.currentTimeMillis();

        // Configure and initialize the JavaStreamingContext
        SparkConf conf = new SparkConf().setMaster("local[2]")
                                        .setAppName("Trending")
                                        .set("spark.driver.host", "localhost");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.minutes(1));

        // ...

        // Execute the Spark workflow defined above
        sc.start();
        sc.awaitTermination();

        System.out.println("\nTime: " + (System.currentTimeMillis() - time) + " ms");
    }
}
