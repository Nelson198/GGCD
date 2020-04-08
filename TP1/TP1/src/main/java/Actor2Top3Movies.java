import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Actor2Top3Movies
 */
public class Actor2Top3Movies {
    /**
     * Left Mapper - Job 1
     * "title.basics.tsv"
     * (key, value) = (tconst, (L, originalTitle))
     */
    public static class Job1LeftMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (!parts[0].equals("tconst") && !parts[3].equals("originalTitle")) {
                context.write(new Text(parts[0]), new Text("L:" + parts[3]));
            }
        }
    }

    /**
     * Middle Mapper - Job 1
     * "title.principals.tsv"
     * (key, value) = (tconst, (M, nconst))
     */
    public static class Job1MiddleMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (!parts[0].equals("tconst") && !parts[2].equals("nconst")) {
                context.write(new Text(parts[0]), new Text("M:" + parts[2]));
            }
        }
    }

    /**
     * Right Mapper - Job 1
     * "title.ratings.tsv"
     * (key, value) = (tconst, (R, averageRating))
     */
    public static class Job1RightMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (!parts[0].equals("tconst") && !parts[1].equals("averageRating")) {
                context.write(new Text(parts[0]), new Text("R:" + parts[1]));
            }
        }
    }

    /**
     * Reducer - Job 1
     * (key, value) = (nconst, (originalTitle, averageRating))
     */
    public static class Job1Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String idActor = "", titleMovie = "";
            float rating = 0;
            StringBuilder sb = new StringBuilder();
            for (Text t : values) {
                String[] parts = t.toString().split(":");
                switch (parts[0]) {
                    case "L":
                        titleMovie = parts[1];
                        break;
                    case "M":
                        idActor = parts[1];
                        break;
                    case "R":
                        rating = Float.parseFloat(parts[1]);
                        break;
                }
            }
            if (!idActor.equals("") && !titleMovie.equals("") && rating != 0) {
                context.write(new Text(idActor), new Text(sb.append(titleMovie).append(":").append(rating).toString()));
            }
        }
    }

    /**
     * Mapper - Job 2
     * (key, value) = (key, value) <=> identity function
     */
    public static class Job2Mapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    /**
     * Reducer - Job 2
     * (key, value) = (nconst, [(originalTitle, averageRating)])
     *
     * Output redirected to "actors" table
     */
    public static class Job2Reducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Build map ( { originalTitle : averageRating } )
            Map<String, Float> info = new HashMap<>();
            for (Text t : values) {
                String[] parts = t.toString().split(":");
                info.put(parts[0], Float.parseFloat(parts[1]));
            }

            // Sort map by value in descending order
            Map<String, Float> sorted = new LinkedHashMap<>();
            info.entrySet().stream()
                    .sorted(Map.Entry.<String, Float>comparingByValue().reversed())
                    .limit(3)
                    .forEachOrdered(x -> sorted.put(x.getKey(), x.getValue()));

            Put put = new Put(Bytes.toBytes(key.toString()));
            int count = 1;
            for (Map.Entry<String, Float> pair : sorted.entrySet()) {
                put.addColumn(Bytes.toBytes("movies"), Bytes.toBytes("top3#" + count++), Bytes.toBytes(pair.getKey()));
            }

            context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        long time = System.currentTimeMillis();

        // Job 1 - "Join"
        Job job1 = Job.getInstance(new Configuration(), "Actor2Top3Movies-Job1");

        // Mapper
        job1.setJarByClass(Actor2Top3Movies.class);

        MultipleInputs.addInputPath(job1, new Path("hdfs://namenode:9000/data/title.basics.tsv"), TextInputFormat.class, Job1LeftMapper.class);
        MultipleInputs.addInputPath(job1, new Path("hdfs://namenode:9000/data/title.principals.tsv"), TextInputFormat.class, Job1MiddleMapper.class);
        MultipleInputs.addInputPath(job1, new Path("hdfs://namenode:9000/data/title.ratings.tsv"), TextInputFormat.class, Job1RightMapper.class);
        job1.setReducerClass(Job1Reducer.class);

        // Reducer
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        TextOutputFormat.setOutputPath(job1, new Path("hdfs://namenode:9000/results/out-Actor2Top3Movies-Job1"));

        boolean ok = job1.waitForCompletion(true);
        if (!ok) {
            throw new IOException("Error with job \"Actor2Top3Movies-Job1\" !");
        }

        // Job 2 configuration
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "zoo");

        // Job 2 - "Group by"
        FileSystem hdfs = FileSystem.get(new URI("hdfs://namenode:9000"), conf);
        Job job2 = Job.getInstance(conf, "Actor2Top3Movies-Job2");

        // Mapper
        job2.setJarByClass(Actor2Top3Movies.class);
        job2.setMapperClass(Job2Mapper.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        TextInputFormat.setInputPaths(job2, "hdfs://namenode:9000/results/out-Actor2Top3Movies-Job1/part-r-00000");

        // Reducer
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        TableMapReduceUtil.initTableReducerJob("actors", Job2Reducer.class, job2);
        job2.setNumReduceTasks(1);

        ok = job2.waitForCompletion(true);
        if (!ok) {
            throw new IOException("Error with job \"Actor2Top3Movies-Job2\" !");
        }

        System.out.println("\nTime: " + (System.currentTimeMillis() - time) + " ms");

        // Delete temporary MapReduce result from Hadoop HDFS
        Path p = new Path("hdfs://namenode:9000/results");
        if (hdfs.exists(p)) {
            hdfs.delete(p, true);
        }
    }
}
