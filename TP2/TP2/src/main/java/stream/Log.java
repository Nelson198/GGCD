package stream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
                  .format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH-mm"));
    }

    public static void main(String[] args) throws InterruptedException {
        // Configure and initialize the JavaStreamingContext
        SparkConf conf = new SparkConf().setAppName("Log");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.minutes(1));

        // Streamgen
        JavaDStream<String> jds = sc.socketTextStream("streamgen", 12345)
                                    .transform((rdd, time) ->
                                        rdd.map(l -> {
                                            String[] parts = l.split("\t");
                                            return String.join("\t", parts[0], parts[1], format(LocalDateTime.ofEpochSecond(time.milliseconds()/1000, 0, ZoneOffset.UTC)));
                                        }))
                                    .window(Durations.minutes(10), Durations.minutes(10));

        // Process streaming data
        AtomicInteger i = new AtomicInteger(1);
        jds.foreachRDD(rdd -> {
            rdd.coalesce(1)
               .saveAsTextFile("hdfs://namenode:9000/Log/Lot" + (i.getAndIncrement()) + " - " + format(LocalDateTime.now()));
        });

        // Execute the Spark workflow defined above
        sc.start();
        sc.awaitTermination();

        // Close streaming context
        sc.close();
    }
}
