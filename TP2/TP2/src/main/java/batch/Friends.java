package batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Friends
 */
public class Friends {
    public static void main(String[] args) {
        long time = System.currentTimeMillis();

        // Spark configuration
        SparkConf conf = new SparkConf().setAppName("Friends");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Initial processing of the "title.principals.tsv.bz2" file
        JavaPairRDD<String, String> jprdd = sc.textFile("hdfs://namenode:9000/data/title.principals.tsv.bz2")
                                              .map(l -> l.split("\t"))
                                              .filter(l -> !l[0].equals("tconst") && !l[2].equals("nconst"))
                                              .mapToPair(l -> new Tuple2<>(l[0], l[2]));

        // Get set of collaborators
        List<Tuple2<String, Iterable<String>>> result = jprdd.groupByKey()
                                                             .flatMapToPair(l -> {
                                                                 Set<Tuple2<String, String>> pairs = new HashSet<>();

                                                                 // Cartesian Product
                                                                 // TODO : Verificar se existe uma forma ainda mais eficiente
                                                                 List<String> aux = new ArrayList<>();
                                                                 l._2.forEach(aux::add);

                                                                 for (int i = 0; i < aux.size(); i++) {
                                                                     String s = aux.get(i);
                                                                     for (int j = i + 1; j < aux.size(); j++) {
                                                                         pairs.add(new Tuple2<>(s, aux.get(j)));
                                                                         pairs.add(new Tuple2<>(aux.get(j), s));
                                                                     }
                                                                 }
                                                                 return pairs.iterator();
                                                             })
                                                             .groupByKey()
                                                             .collect();

        // Output result
        System.out.println("Set of collaborators for each actor:\n");
        for (Tuple2<String, Iterable<String>> t : result) {
            System.out.println(t._1 + " : " + t._2.toString());
        }
        System.out.println();

        // Close spark context
        sc.close();

        System.out.println("\nTime: " + (System.currentTimeMillis() - time) + " ms");
    }
}
