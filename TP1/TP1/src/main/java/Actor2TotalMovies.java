import com.google.common.collect.Iterators;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Actor2TotalMovies
 */
public class Actor2TotalMovies {
    /**
     * Mapper - Job
     * title.principals.tsv
     * (key, value) = (nconst, tconst)
     */
    public static class JobMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (!parts[0].equals("tconst") && !parts[2].equals("nconst")) {
                context.write(new Text(parts[2]), new Text(parts[0]));
            }
        }
    }

    /**
     * Reducer - Job
     * title.principals.tsv
     * (key, value) = (nconst, totalMovies)
     */
    public static class JobReducer extends Reducer<Text, Text, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, new IntWritable(Iterators.size(values.iterator())));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        long time = System.currentTimeMillis();

        // Job - "Total movies for each actor"
        Job job = Job.getInstance(new Configuration(), "Actor2TotalMovies");

        job.setJarByClass(Actor2TotalMovies.class);
        job.setMapperClass(Actor2TotalMovies.JobMapper.class);
        job.setReducerClass(Actor2TotalMovies.JobReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, "hdfs://namenode:9000/data/title.principals.tsv");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("out-Actor2TotalMovies"));

        job.waitForCompletion(true);

        System.out.println("\nTime: " + (System.currentTimeMillis() - time) + " ms");
    }
}
