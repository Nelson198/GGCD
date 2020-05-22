package stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * Trending
 * TODO - Acabar implementação.
 *        Testar 1º localmente e depois com recurso ao docker.
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

        // Initial processing of the "title.ratings.tsv.bz2" file
        JavaMapWithStateDStream<String, Integer, Integer, Integer> ds = sc.socketTextStream("localhost", 12345)
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
                                                        if (!v.isPresent()) {
                                                            s.remove();
                                                        } else {
                                                            boolean trending = true;
                                                            if (s.exists())
                                                                trending = v.get() > s.get();

                                                            if (trending)
                                                                System.out.println("Kebolas krending");

                                                            s.update(v.get());
                                                        }
                                                        return null;
                                                    }
                                                ));

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
