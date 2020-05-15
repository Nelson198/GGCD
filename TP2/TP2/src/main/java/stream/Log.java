package stream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * Log
 * TODO - Acabar
 */
public class Log {
    public static void main(String[] args) throws InterruptedException {
        long time = System.currentTimeMillis();

        // Configure and initialize the JavaStreamingContext
        SparkConf conf = new SparkConf().setMaster("local[2]")
                                        .setAppName("Top3")
                                        .set("spark.driver.host", "localhost");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.minutes(1));

        // Initial processing of the "title.ratings.tsv.gz" file
        JavaPairDStream<String, Float> jpds = sc.socketTextStream("localhost", 12345)
                                                .map(l -> l.split("\t"))
                                                .filter(l -> !l[0].equals("tconst") && !l[1].equals("averageRating"))
                                                .mapToPair(l -> new Tuple2<>(l[0], Float.parseFloat(l[1])))
                                                .window(Durations.minutes(10), Durations.minutes(1));

        // Process streaming data
        jpds.foreachRDD(rdd -> rdd.saveAsTextFile("out-Log"));

        // Execute the Spark workflow defined above
        sc.start();
        sc.awaitTermination();

        System.out.println("\nTime: " + (System.currentTimeMillis() - time) + " ms");
    }
}
