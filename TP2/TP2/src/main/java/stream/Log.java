package stream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Log
 * TODO - Acabar
 */
public class Log {
    public static void main(String[] args) throws InterruptedException {
        // Configure and initialize the JavaStreamingContext
        SparkConf conf = new SparkConf().setMaster("local[2]")
                                        .setAppName("Top3")
                                        .set("spark.driver.host", "localhost");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.minutes(1));

        // Initial processing of the "title.ratings.tsv.gz" file
        JavaPairDStream<String, Tuple2<Integer, LocalDateTime>> jpds = sc
                .socketTextStream("localhost", 12345)
                .map(l -> l.split("\t"))
                .mapToPair(l -> new Tuple2<>(l[0], new Tuple2<>(Integer.parseInt(l[1]), LocalDateTime.now().truncatedTo(ChronoUnit.MINUTES))))
                .window(Durations.minutes(10), Durations.minutes(10));

        // Process streaming data
        jpds.foreachRDD(rdd -> rdd.saveAsTextFile("out-Log"));

        // Execute the Spark workflow defined above
        sc.start();
        sc.awaitTermination();

        // Close streaming context
        sc.close();
    }
}
