package stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Serializable;
import scala.Tuple2;

import java.util.Comparator;

/**
 * Top3
 */
public class Top3 {
    /**
     * Tuple2's Comparator
     */
    public static class MyComparator implements Serializable, Comparator<Tuple2<String, Tuple2<Float, String>>> {
        @Override
        public int compare(Tuple2<String, Tuple2<Float, String>> t1, Tuple2<String, Tuple2<Float, String>> t2) {
            return t1._1.compareTo(t2._1);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        long time = System.currentTimeMillis();

        // Configure and initialize the JavaStreamingContext
        SparkConf conf = new SparkConf().setAppName("Top3");
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
        JavaPairDStream<String, Float> ds = sc.socketTextStream("streamgen", 12345)
                                              .map(l -> l.split("\t"))
                                              .mapToPair(l -> new Tuple2<>(l[0], new Tuple2<>(Integer.parseInt(l[1]), 1)))
                                              .reduceByKeyAndWindow(
                                                  (r1, r2) -> new Tuple2<>(r1._1 + r2._1, r1._2 + r2._2),
                                                  (r1, r2) -> new Tuple2<>(r1._1 - r2._1, r1._2 - r2._2),
                                                  Durations.minutes(10),
                                                  Durations.minutes(1)
                                              )
                                              .mapToPair(r -> new Tuple2<>(r._1, (float) r._2._1 / r._2._2));

        // Join data
        JavaPairDStream<String, Tuple2<Float, String>> joined = ds.transformToPair(rdd -> rdd.join(jprdd));

        // Process joined data
        joined.foreachRDD(rdd -> {
            StringBuilder sb = new StringBuilder("\nTop 3 movie titles with the best average rating:\n\n");
            for (Tuple2<String, Tuple2<Float, String>> t : rdd.top(3, new MyComparator())) {
                sb.append(t._2._2).append("\t(").append(t._1).append(", ").append(t._2._1).append(")").append("\n");
            }
            System.out.println(sb.toString());
        });

        // Execute the Spark workflow defined above
        sc.start();
        sc.awaitTermination();

        // Close streaming context
        sc.close();

        System.out.println("\nTime: " + (System.currentTimeMillis() - time) + " ms");
    }
}
