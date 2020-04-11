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
 * Movie2Details
 */
public class Movie2Details {
    /**
     * Job - Mapper
     * "title.basics.tsv"
     * (key, value) = (null, Put)
     */
    public static class MyMapper extends Mapper<LongWritable, Text, NullWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");

            if (data[0].equals("tconst")) return;

            Put put = new Put(Bytes.toBytes(data[0]));
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("titleType"), Bytes.toBytes(data[1]));
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("primaryTitle"), Bytes.toBytes(data[2]));
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("originalTitle"), Bytes.toBytes(data[3]));
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("isAdult"), Bytes.toBytes(data[4]));
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("startYear"), Bytes.toBytes(data[5]));
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("endYear"), Bytes.toBytes(data[6]));
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("runtimeMinutes"), Bytes.toBytes(data[7]));
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("genres"), Bytes.toBytes(data[8]));

            context.write(null, put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        long time = System.currentTimeMillis();

        // Job configuration
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "zoo");

        // Job - Insert into "movies" table info from "title.basics.tsv"
        Job job = Job.getInstance(conf, "Movie2Details");

        // Mapper
        job.setJarByClass(Movie2Details.class);
        job.setMapperClass(MyMapper.class);

        job.setNumReduceTasks(0);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("hdfs://namenode:9000/data/title.basics.tsv"));

        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "movies");

        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw new IOException("Error with job \"Movies2Details\" !");
        }

        System.out.println((System.currentTimeMillis() - time) + " ms");
    }
}