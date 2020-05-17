package stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import java.util.List;

/**
 * Trending
 * TODO - Acabar
 */
public class Trending {
    public static void main(String[] args) throws InterruptedException {
        // Configure and initialize the JavaStreamingContext
        SparkConf conf = new SparkConf().setMaster("local[2]")
                                        .setAppName("Trending")
                                        .set("spark.driver.host", "localhost");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.minutes(1));

        // Receive streaming data from the sources

        // Initial processing of the "title.basics.tsv.gz" file
        JavaPairRDD<String, String> jprdd = sc.sparkContext()
                                              .textFile("../data/title.basics.tsv.gz")
                                              .map(l -> l.split("\t"))
                                              .filter(l -> !l[0].equals("tconst") && !l[3].equals("originalTitle"))
                                              .mapToPair(l -> new Tuple2<>(l[0], l[3]))
                                              .cache();

        // Initial processing of the "title.ratings.tsv.gz" file
        JavaPairDStream<String, Integer> ds = sc.socketTextStream("localhost", 12345)
                                                .map(l -> l.split("\t"))
                                                .mapToPair(l -> new Tuple2<>(l[0], Integer.parseInt(l[1])))
                                                .window(Durations.minutes(15), Durations.minutes(15));

                                                /*
                                                .mapWithState(StateSpec.function(
                                                    (String k, Optional<Integer> v, State<Integer> s) -> {
                                                        int count = s.exists() ? s.get() : v.get();
                                                        s.update(count);
                                                        return v.get() > count;
                                                    }
                                                ))
                                                */

                                                // TODO - Ver slides 140-143 (8-Streaming.pdf)

        // Join data
        JavaPairDStream<String, Tuple2<Integer, String>> joined = ds.transformToPair(rdd -> rdd.join(jprdd));

        // Process joined data
        joined.mapToPair(t -> new Tuple2<>(t._2._1, new Tuple2<>(t._2._2, t._1)))
              .foreachRDD(rdd -> {
                  StringBuilder sb = new StringBuilder("\nTrending movie titles:\n\n");
                  for (Tuple2<Integer, Tuple2<String, String>> t : rdd.collect()) {
                      sb.append(t._2._1).append("\t(").append(t._2._2).append(", ").append(t._1).append(")").append("\n");
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
