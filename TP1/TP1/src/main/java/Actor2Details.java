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

public class Actor2Details {
    /**
     * Mapper - Job
     * Input  : "name.basics.tsv"
     * Output : (key, value) = (null, Put)
     */
    public static class JobMapper extends Mapper<LongWritable, Text, NullWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");

            if (key.get() == 0) return;

            Put put = new Put(Bytes.toBytes(data[0]));
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("primaryName"), Bytes.toBytes(data[1]));
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("birthYear"), Bytes.toBytes(data[2]));
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("deathYear"), Bytes.toBytes(data[3]));

            context.write(null, put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        long time = System.currentTimeMillis();

        // Job configuration
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "zoo");

        // Job - Insert into "actors" table info from "name.basics.tsv"
        Job job = Job.getInstance(conf, "Actor2Details");

        // Mapper
        job.setJarByClass(Actor2Details.class);
        job.setMapperClass(JobMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("hdfs://namenode:9000/data/name.basics.tsv"));

        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "actors");

        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw new IOException("Error with job \"Actor2Details\" !");
        }

        System.out.println((System.currentTimeMillis() - time) + " ms");
    }
}
