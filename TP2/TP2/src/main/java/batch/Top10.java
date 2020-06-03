package batch;

import com.google.common.collect.Iterables;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Top10
 */
public class Top10 {
    /**
     * Tuple2's Comparator
     */
    public static class MyComparator implements Serializable, Comparator<Tuple2<Integer, String>> {
        @Override
        public int compare(Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) {
            return t1._1.compareTo(t2._1);
        }
    }

    public static void main(String[] args) {
        long time = System.currentTimeMillis();

        // Spark configuration
        SparkConf conf = new SparkConf().setAppName("Top10");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Initial processing of the "title.principals.tsv" file
        List<Tuple2<Integer, String>> top10 = sc.textFile("hdfs://namenode:9000/data/title.principals.tsv")
                                                .map(l -> l.split("\t"))
                                                .filter(l -> !l[0].equals("tconst") && !l[2].equals("nconst"))
                                                .mapToPair(l -> new Tuple2<>(l[2], l[0]))
                                                .groupByKey()
                                                .mapToPair(p -> new Tuple2<>(Iterables.size(p._2), p._1))
                                                .top(10, new MyComparator());

        List<String> top10ActorsIds = new ArrayList<>();
        top10.forEach(t -> top10ActorsIds.add(t._2));

        // Initial processing of the "name.basics.tsv" file
        Map<String, String> names = sc.textFile("hdfs://namenode:9000/data/name.basics.tsv")
                                      .map(l -> l.split("\t"))
                                      .filter(l -> !l[0].equals("nconst") && !l[1].equals("primaryName") && top10ActorsIds.contains(l[0]))
                                      .mapToPair(l -> new Tuple2<>(l[0], l[1]))
                                      .collectAsMap();

        // Output result
        System.out.println("\nTop 10:\n");
        for (Tuple2<Integer, String> t : top10) {
            System.out.println(names.get(t._2) + " : " + t._1);
        }
        System.out.println();

        // Close spark context
        sc.close();

        System.out.println("\nTime: " + (System.currentTimeMillis() - time) + " ms");
    }
}
