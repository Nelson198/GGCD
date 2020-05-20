package batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.spark_project.guava.collect.Iterators;

import scala.Tuple2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Ratings
 */
public class Ratings {
    /**
     * Get log's information, joining all the lots associated.
     */
    public static void getLog() {
        try {
            // Check if "Log.txt" exists
            File file  = new File("Log/Log.txt");
            if (file.exists()) {
                System.out.println("[INFO] File \"" + file.getName() + "\" already exists.");
            } else {
                // Create instance of directory
                File directory = new File("Log");

                // Get list of all folders
                String[] folders = directory.list((current, name) -> new File(current, name).isDirectory());

                if (folders != null && folders.length != 0) {
                    Arrays.sort(folders);
                } else if (folders != null) {
                    System.out.println("[INFO] Folder \"" + directory.getName() + "\" doesn't contain information regarding the logs.\nPlease run the file \"Log.java\".");
                    System.exit(0);
                } else {
                    System.err.println("[ERROR] Folder \"" + directory.getName() + "\" doesn't exist.");
                    System.exit(1);
                }

                // Append the log's information in "Log.txt"
                BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));

                for (String folder : folders) {
                    BufferedReader br = new BufferedReader(new FileReader("Log/" + folder + "/part-00000"));

                    String s = br.readLine();
                    while (s != null) {
                        bw.write(s);
                        bw.newLine();
                        s = br.readLine();
                    }
                    br.close();
                }
                bw.close();

                System.out.println("[INFO] File \"" + file.getName() + "\" was created successfully.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Save the file "Ratings/part-00000" in "title.ratings.new.tsv"
     */
    public static void save() {
        try {
            File file = new File("Ratings/title.ratings.new.tsv");

            BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
            BufferedReader br = new BufferedReader(new FileReader("Ratings/part-00000"));

            // Add header
            bw.write("tconst\taverageRating\tnumVotes");
            bw.newLine();

            String s = br.readLine();
            while (s != null) {
                bw.write(s);
                bw.newLine();
                s = br.readLine();
            }
            br.close();
            bw.close();

            System.out.println("[INFO] File \"" + file.getName() + "\" was created successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        long time = System.currentTimeMillis();

        // Get log's information
        getLog();

        // Spark configuration
        SparkConf conf = new SparkConf().setAppName("Ratings")
                                        .setMaster("local")
                                        .set("spark.driver.host", "localhost");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Initial processing of the "title.ratings.tsv" file
        JavaPairRDD<String, Tuple2<Float, Integer>> jprdd1 = sc.textFile("../data/title.ratings.tsv.gz")
                                                               .map(l -> l.split("\t"))
                                                               .filter(l -> !l[0].equals("tconst") && !l[1].equals("averageRating") && !l[2].equals("numVotes"))
                                                               .mapToPair(l -> new Tuple2<>(l[0], new Tuple2<>(Float.parseFloat(l[1]) * Integer.parseInt(l[2]), Integer.parseInt(l[2]))));

        JavaPairRDD<String, Tuple2<Integer, Integer>> jprdd2 = sc.textFile("Log/Log.txt")
                                                                 .map(l -> {
                                                                     String[] data = l.substring(1, l.length() - 1).split(",\\(");
                                                                     return new String[] {data[0], data[1].split(",")[0]};
                                                                 })
                                                                 .mapToPair(l -> new Tuple2<>(l[0], Integer.parseInt(l[1])))
                                                                 .groupByKey()
                                                                 .mapToPair(l -> {
                                                                     AtomicInteger votes = new AtomicInteger();
                                                                     l._2.forEach(votes::addAndGet);
                                                                     return new Tuple2<>(l._1, new Tuple2<>(Iterators.size(l._2.iterator()), votes.get()));
                                                                 });

        // Join data
        JavaPairRDD<String, Tuple2<Tuple2<Float, Integer>, Tuple2<Integer, Integer>>> joined = jprdd1.join(jprdd2);

        // Process joined data
        joined.map(p -> p._1 + "\t" + String.format("%.1f", (p._2._1._1 + p._2._2._2) / (p._2._1._2 + p._2._2._1)) + "\t" + (p._2._1._2 + p._2._2._1))
              .saveAsTextFile("Ratings");

        // Close spark context
        sc.close();

        // Produce "title.ratings.new.tsv"
        save();

        System.out.println("\nTime: " + (System.currentTimeMillis() - time) + " ms");
    }
}
