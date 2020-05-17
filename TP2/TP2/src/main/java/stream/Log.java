package stream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Log
 */
public class Log {
    /**
     * Format date and time
     * @param ldt LocalDateTime
     * @return Formatted date and time
     */
    public static String format(LocalDateTime ldt) {
        return ldt.truncatedTo(ChronoUnit.MINUTES)
                  .format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm"));
    }

    public static void main(String[] args) throws InterruptedException {
        // Configure and initialize the JavaStreamingContext
        SparkConf conf = new SparkConf().setMaster("local[2]")
                                        .setAppName("Top3")
                                        .set("spark.driver.host", "localhost");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.minutes(1));

        // Initial processing of the "title.ratings.tsv.gz" file
        JavaPairDStream<String, Tuple2<Integer, String>> jpds = sc
            .socketTextStream("localhost", 12345)
            .map(l -> l.split("\t"))
            .mapToPair(l -> new Tuple2<>(l[0], new Tuple2<>(Integer.parseInt(l[1]), format(LocalDateTime.now()))))
            .window(Durations.minutes(1), Durations.minutes(1));

        // Process streaming data
        AtomicInteger i = new AtomicInteger(1);
        jpds.foreachRDD(rdd -> rdd.coalesce(1).saveAsTextFile("Log/Lot" + (i.getAndIncrement()) + " - " + format(LocalDateTime.now())));

        // Execute the Spark workflow defined above
        sc.start();
        sc.awaitTermination();

        // Close streaming context
        sc.close();
    }
}
