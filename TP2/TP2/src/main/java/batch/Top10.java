package batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Serializable;
import scala.Tuple2;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.List;

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

        // Initial processing of the "title.principals.tsv.bz2" file
        JavaPairRDD<String, String> jprdd1 = sc.textFile("hdfs://namenode:9000/data/title.principals.tsv.bz2")
                                               .map(l -> l.split("\t"))
                                               .filter(l -> !l[0].equals("tconst") && !l[2].equals("nconst"))
                                               .mapToPair(l -> new Tuple2<>(l[2], l[0]));

        // Initial processing of the "name.basics.tsv.bz2" file
        JavaPairRDD<String, String> jprdd2 = sc.textFile("hdfs://namenode:9000/data/name.basics.tsv.bz2")
                                               .map(l -> l.split("\t"))
                                               .filter(l -> !l[0].equals("nconst") && !l[1].equals("primaryName"))
                                               .mapToPair(l -> new Tuple2<>(l[0], l[1]));

        // Join data
        JavaPairRDD<String, String> joined = jprdd2.join(jprdd1)
                                                   .mapToPair(p -> new Tuple2<>(p._2._1, p._2._2));

        // Process joined data
        List<Tuple2<Integer, String>> result = joined.groupByKey()
                                                     .mapToPair(p -> {
                                                         Set<String> h = new HashSet<>();
                                                         p._2.forEach(h::add);
                                                         return new Tuple2<>(p._1, h);
                                                     })
                                                     .mapToPair(p -> new Tuple2<>(p._1, p._2.size()))
                                                     .mapToPair(p -> new Tuple2<>(p._2, p._1))
                                                     .top(10, new MyComparator());

        // Output result
        System.out.println("\nTop 10:\n");
        for (Tuple2<Integer, String> t : result) {
            System.out.println(t._2 + " : " + t._1);
        }
        System.out.println();

        // Close spark context
        sc.close();

        System.out.println("\nTime: " + (System.currentTimeMillis() - time) + " ms");
    }
}
