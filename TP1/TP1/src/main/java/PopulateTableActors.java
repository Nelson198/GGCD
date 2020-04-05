import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * PopulateTableActors
 */
public class PopulateTableActors {
    public static class Job1Mapper extends Mapper<LongWritable, Text, NullWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");

            if (data[0].equals("nconst")) return;

            Put put = new Put(Bytes.toBytes(data[0]));
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("name"), Bytes.toBytes(data[1]));
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("birthYear"), Bytes.toBytes(data[2]));
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("deathYear"), Bytes.toBytes(data[3]));

            context.write(null, put);
        }
    }

    public static class Job2Mapper extends Mapper<LongWritable, Text, NullWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");

            Put put = new Put(Bytes.toBytes(data[0]));
            put.addColumn(Bytes.toBytes("movies"), Bytes.toBytes("total"), Bytes.toBytes(data[1]));

            context.write(null, put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        long time = System.currentTimeMillis();

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "zoo");

        // Job 1 - Insert into "actors" info from "name.basics.tsv"
        Job job1 = Job.getInstance(conf, "PopulateTableActors1");

        job1.setJarByClass(PopulateTableActors.class);
        job1.setMapperClass(PopulateTableActors.Job1Mapper.class);

        job1.setNumReduceTasks(0);
        job1.setOutputKeyClass(ImmutableBytesWritable.class);
        job1.setOutputValueClass(Put.class);

        job1.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job1, new Path("hdfs://namenode:9000/data/name.basics.tsv"));

        job1.setOutputFormatClass(TableOutputFormat.class);
        job1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "actors");

        job1.waitForCompletion(true);

        // Job 2 - Insert into "actors" info from "Actor2TotalMovies"
        Job job2 = Job.getInstance(conf, "PopulateTableActors2");

        job2.setJarByClass(PopulateTableActors.class);
        job2.setMapperClass(PopulateTableActors.Job2Mapper.class);

        job2.setNumReduceTasks(0);
        job2.setOutputKeyClass(ImmutableBytesWritable.class);
        job2.setOutputValueClass(Put.class);

        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job2, new Path("hdfs://namenode:9000/results/out-Actor2TotalMovies/part-r-00000"));

        job2.setOutputFormatClass(TableOutputFormat.class);
        job2.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "actors");

        job2.waitForCompletion(true);

        System.out.println((System.currentTimeMillis() - time) + " ms");
    }
}
