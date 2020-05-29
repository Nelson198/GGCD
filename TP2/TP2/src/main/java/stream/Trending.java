package stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * Trending
 */
public class Trending {
    public static void main(String[] args) throws InterruptedException {
        // Configure and initialize the JavaStreamingContext
        SparkConf conf = new SparkConf().setAppName("Trending");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.minutes(1));
        sc.checkpoint("hdfs://namenode:9000/checkpoint");

        // Receive streaming data from the sources
        // Initial processing of the "title.basics.tsv" file
        JavaPairRDD<String, String> jprdd = sc.sparkContext()
                                              .textFile("hdfs://namenode:9000/data/title.basics.tsv")
                                              .map(l -> l.split("\t"))
                                              .filter(l -> !l[0].equals("tconst") && !l[3].equals("originalTitle"))
                                              .mapToPair(l -> new Tuple2<>(l[0], l[3]))
                                              .cache();

        // Streamgen
        JavaPairDStream<String, Boolean> ds = sc.socketTextStream("streamgen", 12345)
                                                .map(l -> l.split("\t"))
                                                .mapToPair(l -> new Tuple2<>(l[0], 1))
                                                .reduceByKeyAndWindow(
                                                    (a, b) -> a + b,
                                                    (a, b) -> a - b,
                                                    Durations.minutes(15),
                                                    Durations.minutes(15)
                                                )
                                                .mapWithState(StateSpec.function(
                                                    (String k, Optional<Integer> v, State<Integer> s) -> {
                                                        boolean trending;
                                                        if (!v.isPresent()) {
                                                            s.remove();
                                                            trending = false;
                                                        } else {
                                                            if (s.exists())
                                                                trending = v.get() > s.get();
                                                            else
                                                                trending = true;
                                                            s.update(v.get());
                                                        }
                                                        return new Tuple2<>(k, trending);
                                                    }
                                                ))
                                                .mapToPair(t -> t);

        // Join data
        JavaDStream<String> joined = ds.transformToPair(rdd -> jprdd.join(rdd.filter(t -> t._2)))
                                       .map(t -> t._2._1);

        // Process joined data
        joined.foreachRDD(rdd -> {
            StringBuilder sb = new StringBuilder("\nTrending movie titles:\n\n");
            for (String movie : rdd.collect()) {
                sb.append(movie).append("\n");
            }
            System.out.println(sb.toString());
        });

        // Execute the Spark workflow defined above
        sc.start();
        sc.awaitTermination();

        // Close streaming context
        sc.close();
    }
}
