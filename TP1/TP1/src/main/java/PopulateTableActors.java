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
    public static class JobMapper extends Mapper<LongWritable, Text, NullWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");

            if (data[0].equals("nconst")) return;

            Put put = new Put(Bytes.toBytes(data[0]));
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("name"), Bytes.toBytes(data[1]));
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("birthYear"), Bytes.toBytes(data[2]));
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("deathYear"), Bytes.toBytes(data[3]));

            put.addColumn(Bytes.toBytes("movies"), Bytes.toBytes("total"), Bytes.toBytes(String.valueOf(data[5].split(",").length)));

            // Fazer Top3

            context.write(null, put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        long time = System.currentTimeMillis();

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "zoo");

        Job job = Job.getInstance(conf, "PopulateActors");

        job.setJarByClass(PopulateTableActors.class);
        job.setMapperClass(PopulateTableActors.JobMapper.class);

        job.setNumReduceTasks(0);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("hdfs://namenode:9000/data/name.basics.tsv"));

        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "actors");

        job.waitForCompletion(true);

        System.out.println((System.currentTimeMillis() - time) + " ms");
    }
}
