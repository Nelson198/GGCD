import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Actor2Top3Movies
 */
public class Actor2Top3Movies {
    /**
     * Left Mapper - Job 1
     * title.basics.tsv
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
     * title.principals.tsv
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
     * title.ratings.tsv
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
     */
    public static class Job2Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
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

            // Output result
            StringBuilder sb = new StringBuilder("[");
            for (Map.Entry<String, Float> pair : sorted.entrySet()) {
                sb.append("(").append(pair.getKey()).append(", ").append(pair.getValue()).append("), ");
            }
            sb.append("]");
            sb.delete(sb.length() - 3, sb.length() - 1);

            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        long time = System.currentTimeMillis();

        // Job 1 - "Join"
        Job job1 = Job.getInstance(new Configuration(), "Actor2Top3Movies-Job1");

        job1.setJarByClass(Actor2Top3Movies.class);

        MultipleInputs.addInputPath(job1, new Path("hdfs://namenode:9000/data/title.basics.tsv"), TextInputFormat.class, Job1LeftMapper.class);
        MultipleInputs.addInputPath(job1, new Path("hdfs://namenode:9000/data/title.principals.tsv"), TextInputFormat.class, Job1MiddleMapper.class);
        MultipleInputs.addInputPath(job1, new Path("hdfs://namenode:9000/data/title.ratings.tsv"), TextInputFormat.class, Job1RightMapper.class);
        job1.setReducerClass(Job1Reducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        TextOutputFormat.setOutputPath(job1, new Path("hdfs://namenode:9000/results/out-Actor2Top3Movies-Job1"));

        job1.waitForCompletion(true);

        // Job 2 - "Group by"
        Job job2 = Job.getInstance(new Configuration(), "Actor2Top3Movies-Job2");

        job2.setJarByClass(Actor2Top3Movies.class);
        job2.setMapperClass(Job2Mapper.class);
        job2.setReducerClass(Job2Reducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        TextInputFormat.setInputPaths(job2, "hdfs://namenode:9000/results/out-Actor2Top3Movies-Job1/part-r-00000");

        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2, new Path("out-Actor2Top3Movies-Job2"));

        job2.waitForCompletion(true);

        System.out.println("\nTime: " + (System.currentTimeMillis() - time) + " ms");
    }
}
