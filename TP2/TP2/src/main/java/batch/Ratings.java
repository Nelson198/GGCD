package batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Ratings
 * TODO - Acabar
 */
public class Ratings {
    public static void main(String[] args) {
        long time = System.currentTimeMillis();

        // Spark configuration
        SparkConf conf = new SparkConf().setAppName("Ratings");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Initial processing of the "title.ratings.tsv" file
        JavaPairRDD<String, String> jprdd = sc.textFile("hdfs://namenode:9000/data/title.ratings.tsv")
                                              .map(l -> l.split("\t"))
                                              .filter(l -> !l[0].equals("tconst") && !l[1].equals("averageRating") && !l[2].equals("numVotes"))
                                              .mapToPair(l -> new Tuple2<>(l[0], l[2]));

        // ...

        // Close spark context
        sc.close();

        System.out.println("\nTime: " + (System.currentTimeMillis() - time) + " ms");
    }
}
