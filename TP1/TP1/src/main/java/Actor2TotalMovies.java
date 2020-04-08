import com.google.common.collect.Iterators;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * Actor2TotalMovies
 */
public class Actor2TotalMovies {
    /**
     * Mapper - Job
     * "title.principals.tsv"
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
     * "title.principals.tsv"
     * (key, value) = (nconst, totalMovies)
     *
     * Output redirected to "actors" table
     */
    public static class JobReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("movies"), Bytes.toBytes("total"), Bytes.toBytes(Iterators.size(values.iterator())));

            context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        long time = System.currentTimeMillis();

        // Job configuration
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "zoo");

        // Job - Total movies for each actor
        Job job = Job.getInstance(conf, "Actor2TotalMovies");

        // Mapper
        job.setJarByClass(Actor2TotalMovies.class);
        job.setMapperClass(JobMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, "hdfs://namenode:9000/data/title.principals.tsv");

        // Reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        TableMapReduceUtil.initTableReducerJob("actors", JobReducer.class, job);
        job.setNumReduceTasks(1);

        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw new IOException("Error with job \"Actor2TotalMovies\" !");
        }

        System.out.println("\nTime: " + (System.currentTimeMillis() - time) + " ms");
    }
}
