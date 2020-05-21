package batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.spark_project.guava.collect.Iterators;

import scala.Tuple2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import java.net.URI;
import java.net.URISyntaxException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Ratings
 */
public class Ratings {
    /**
     * Get log's information, joining all the lots associated.
     */
    public static void getLog(FileSystem fs) {
        try {
            // Check if the file "Log.txt" exists
            Path logOutput = new Path("hdfs://namenode:9000/Log/Log.txt");
            if (fs.exists(logOutput)) {
                System.out.println("[INFO] File \"" + logOutput.getName() + "\" already exists.");
            } else {
                // Create instance of directory
                Path logFolder = new Path("hdfs://namenode:9000/Log");

                // Check if the folder "Log" exists
                if(!fs.exists(logFolder)) {
                    System.err.println("[INFO] Folder \"" + logFolder.getName() + "\" doesn't exist.\nPlease run the file \"Log.java\".");
                    System.exit(0);
                }

                // Get the list of all subfolders
                List<String> folders = new ArrayList<>();
                for (FileStatus fileStatus : fs.listStatus(logFolder)) {
                    if (fileStatus.isDirectory()) {
                        folders.add(fileStatus.getPath().getName());
                    }
                }

                if (folders.size() == 0) {
                    System.out.println("[INFO] Folder \"" + logFolder.getName() + "\" doesn't contain information regarding the logs.\nPlease run the file \"Log.java\".");
                    System.exit(0);
                }

                // Create output file "Log.txt"
                FSDataOutputStream fsdos = fs.create(logOutput, true);

                // Append the log's information in "Log.txt"
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsdos));

                for (String folder : folders) {
                    FSDataInputStream fsdis = fs.open(new Path("hdfs://namenode:9000/Log/" + folder + "/part-00000"));
                    BufferedReader br = new BufferedReader(new InputStreamReader(fsdis));

                    String s = br.readLine();
                    while (s != null) {
                        bw.write(s);
                        bw.newLine();
                        s = br.readLine();
                    }
                    br.close();
                }
                bw.close();

                System.out.println("[INFO] File \"" + logOutput.getName() + "\" was created successfully.");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Save the file "Ratings/part-00000" in "title.ratings.new.tsv"
     */
    public static void save(FileSystem fs) {
        try {
            Path output = new Path("hdfs://namenode:9000/Ratings/title.ratings.new.tsv");

            // Create output file "title.ratings.new.tsv"
            FSDataOutputStream fsdos = fs.create(output, true);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsdos));

            // Read file "Ratings/part-00000"
            FSDataInputStream fsdis = fs.open(new Path("hdfs://namenode:9000/Ratings/part-00000"));
            BufferedReader br = new BufferedReader(new InputStreamReader(fsdis));

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

            System.out.println("[INFO] File \"" + output.getName() + "\" was created successfully.");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        long time = System.currentTimeMillis();

        // Hadoop HDFS's configuration
        Configuration c = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI("hdfs://namenode:9000"), c);
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
            System.exit(1);
        }

        // Get log's information
        getLog(fs);

        // Spark configuration
        SparkConf conf = new SparkConf().setAppName("Ratings");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Initial processing of the "title.ratings.tsv" file
        JavaPairRDD<String, Tuple2<Float, Integer>> jprdd1 = sc.textFile("hdfs://namenode:9000/data/title.ratings.tsv")
                                                               .map(l -> l.split("\t"))
                                                               .filter(l -> !l[0].equals("tconst") && !l[1].equals("averageRating") && !l[2].equals("numVotes"))
                                                               .mapToPair(l -> new Tuple2<>(l[0], new Tuple2<>(Float.parseFloat(l[1]) * Integer.parseInt(l[2]), Integer.parseInt(l[2]))));

        JavaPairRDD<String, Tuple2<Integer, Integer>> jprdd2 = sc.textFile("hdfs://namenode:9000/Log/Log.txt")
                                                                 .map(l -> l.split("\t"))
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
              .saveAsTextFile("hdfs://namenode:9000/Ratings");

        // Close spark context
        sc.close();

        // Produce "title.ratings.new.tsv"
        save(fs);

        System.out.println("\nTime: " + (System.currentTimeMillis() - time) + " ms");
    }
}
