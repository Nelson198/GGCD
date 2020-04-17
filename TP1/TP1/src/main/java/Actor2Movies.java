import com.google.common.collect.Iterators;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
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
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Actor2Movies
 */
public class Actor2Movies {
    /**
     * Left Mapper - Job 1
     * "title.principals.tsv"
     * Output : (key, value) = (tconst, (L, nconst))
     */
    public static class Job1LeftMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (key.get() == 0) return;
            if (parts[3].equals("actor") || parts[3].equals("actress")) {
                context.write(new Text(parts[0]), new Text("L" + parts[2]));
            }
        }
    }

    /**
     * Middle Mapper - Job 1
     * Access "movies" table to get all movies names
     * Output : (key, value) = (tconst, (M, originalTitle))
     */
    public static class Job1MiddleMapper extends TableMapper<Text, Text> {
        public void map(ImmutableBytesWritable key, Result values, Context context) throws IOException, InterruptedException {
            String title = Bytes.toString(values.getValue(Bytes.toBytes("details"), Bytes.toBytes("originalTitle")));
            context.write(new Text(key.toString()), new Text("M" + title));
        }
    }

    /**
     * Right Mapper - Job 1
     * "title.ratings.tsv"
     * Output : (key, value) = (tconst, (R, averageRating))
     */
    public static class Job1RightMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (key.get() == 0) return;
            context.write(new Text(parts[0]), new Text("R" + parts[1]));
        }
    }

    /**
     * Reducer - Job 1
     * Input  : (key, value) = (tconst, [(L, nconst)+, (M, originalTitle){1}, (R, averageRating){1}])
     * Output : (key, value) = (nconst, (originalTitle, averageRating))
     */
    public static class Job1Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> idActors = new ArrayList<>();
            String originalTitle = "";
            float averageRating = 0;
            for (Text t : values) {
                String s = t.toString();
                switch (s.charAt(0)) {
                    case 'L':
                        idActors.add(s.substring(1));
                        break;
                    case 'M':
                        // System.out.println(originalTitle) ✅
                        originalTitle = s.substring(1);
                        break;
                    case 'R':
                        averageRating = Float.parseFloat(s.substring(1));
                        break;
                }
            }
            for (String idActor : idActors) {
                // originalTitle é uma string vazia !!! ❌
                System.out.println(idActor + " --> ( " + originalTitle + ", " + averageRating + " )");
                context.write(new Text(idActor), new Text(originalTitle + ":" + averageRating));
            }
        }
    }

    /**
     * Mapper - Job 2
     * Identity function
     * Output : (key, value) = (nconst, (tconst, averageRating))
     */
    public static class Job2Mapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    /**
     * Reducer - Job 2
     * Input  : (key, value) = (nconst, [(originalTitle, averageRating)])
     * Output : (key, value) = (nconst, Put)
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
            Set<Map.Entry<String, Float>> top3 = info.entrySet()
                    .stream()
                    .sorted(Map.Entry.<String, Float>comparingByValue().reversed().thenComparing(x -> x.getKey()))
                    .limit(3)
                    .collect(Collectors.toSet());

            Put put = new Put(Bytes.toBytes(key.toString()));

            // Top 3 movies for each actor
            int count = 1;
            for (Map.Entry<String, Float> pair : top3) {
                put.addColumn(Bytes.toBytes("movies"), Bytes.toBytes("top3#" + (count++)), Bytes.toBytes(pair.getKey()));
            }

            // Total movies for each actor
            put.addColumn(Bytes.toBytes("movies"), Bytes.toBytes("total"), Bytes.toBytes(Iterators.size(values.iterator())));

            context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        long time = System.currentTimeMillis();

        // Job 1 configuration
        Scan s = new Scan();
        s.addFamily(Bytes.toBytes("details"));

        Configuration conf1 = HBaseConfiguration.create();
        conf1.set("hbase.zookeeper.quorum", "zoo");
        conf1.set(TableInputFormat.INPUT_TABLE, "movies");
        conf1.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(s));

        // Job 1 - "Join"
        Job job1 = Job.getInstance(conf1, "Actor2Movies-Job1");

        // Mappers
        job1.setJarByClass(Actor2Movies.class);

        MultipleInputs.addInputPath(job1, new Path("HT_movies"), TableInputFormat.class, Job1MiddleMapper.class);
        MultipleInputs.addInputPath(job1, new Path("hdfs://namenode:9000/data/title.principals.tsv"), TextInputFormat.class, Job1LeftMapper.class);
        MultipleInputs.addInputPath(job1, new Path("hdfs://namenode:9000/data/title.ratings.tsv"), TextInputFormat.class, Job1RightMapper.class);

        // Reducer
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setReducerClass(Job1Reducer.class);

        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        TextOutputFormat.setOutputPath(job1, new Path("hdfs://namenode:9000/results/out-Actor2Movies-Job1"));

        boolean ok = job1.waitForCompletion(true);
        if (!ok) {
            throw new IOException("Error with job \"Actor2Movies-Job1\" !");
        }

        // Job 2 configuration
        Configuration conf2 = HBaseConfiguration.create();
        conf2.set("hbase.zookeeper.quorum", "zoo");

        // Job 2 - "Group by"
        FileSystem hdfs = FileSystem.get(new URI("hdfs://namenode:9000"), conf2);
        Job job2 = Job.getInstance(conf2, "Actor2Movies-Job2");

        // Mapper
        job2.setJarByClass(Actor2Movies.class);
        job2.setMapperClass(Job2Mapper.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        TextInputFormat.setInputPaths(job2, "hdfs://namenode:9000/results/out-Actor2Movies-Job1/part-r-00000");

        // Reducer
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        TableMapReduceUtil.initTableReducerJob("actors", Job2Reducer.class, job2);
        job2.setNumReduceTasks(1);

        ok = job2.waitForCompletion(true);
        if (!ok) {
            throw new IOException("Error with job \"Actor2Movies-Job2\" !");
        }

        System.out.println("\nTime: " + (System.currentTimeMillis() - time) + " ms");

        // Delete temporary MapReduce result from Hadoop HDFS
        Path p = new Path("hdfs://namenode:9000/results");
        if (hdfs.exists(p)) {
            hdfs.delete(p, true);
        }
    }
}
