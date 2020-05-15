package streamgen;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.sampling.distribution.AhrensDieterExponentialSampler;
import org.apache.commons.rng.sampling.distribution.ContinuousSampler;
import org.apache.commons.rng.simple.RandomSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Main {
    private static Logger log = LoggerFactory.getLogger(Main.class);

    private static class Entry implements Comparable<Entry> {
        public String id;
        public float rating;
        public long votes;

        public Entry(String id, float rating, long votes) {
            this.id = id;
            this.rating = rating;
            this.votes = votes;
        }

        @Override
        public int compareTo(Entry entry) {
            return Long.compare(this.votes, entry.votes);
        }
    }

    public static void main(String[] args) throws Exception {

        String name = args.length < 1 ? "title.ratings.tsv.bz2" : args[0];

        double rate = args.length < 2 ? 120 : Double.parseDouble(args[1]);

        log.info("reading IMDb ratings from {}, generating {} events/min", name, rate);

        InputStream raw;
        FileSystem fs = null;

        if (name.startsWith("hdfs:")) {
            Configuration conf = new Configuration();
            conf.addResource(new Path("/etc/hadoop/core-site.xml"));
            conf.addResource(new Path("/etc/hadoop/hdfs-site.xml"));
            fs = FileSystem.get(conf);
            raw = fs.open(new Path(name));
        } else if (name.startsWith("http:") || name.startsWith("https:")) {
            raw = new URL(name).openStream();
        } else {
            raw = new FileInputStream(name);
        }
        raw = new BufferedInputStream(raw);

        try {
            CompressorStreamFactory csf = new CompressorStreamFactory();
            String format = csf.detect(raw);
            log.info("detected file compressed with {}", format);
            raw = csf.createCompressorInputStream(format, raw);
        } catch (CompressorException ce) {
            log.info("no compression detected, parsing raw file");
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(raw));

        List<Entry> titles = new ArrayList<>();

        br.readLine(); // discard header
        String line = br.readLine();
        long votes = 0;
        while (line != null) {
            String[] row = line.split("\\t");
            votes += Integer.parseInt(row[2]);
            titles.add(new Entry(row[0], Float.parseFloat(row[1]), votes));
            line = br.readLine();
        }

        br.close();
        if (fs != null)
            fs.close();

        log.info("read {} lines", titles.size());

        UniformRandomProvider rng = RandomSource.create(RandomSource.MT_64);
        ContinuousSampler iat = new AhrensDieterExponentialSampler(rng, 1 / rate);

        ServerSocket ss = new ServerSocket(12345);
        while (true) {
            log.info("waiting for connection on {}", ss.getLocalSocketAddress());
            try (Socket s = ss.accept()) {
                log.info("new connection from {}", s.getRemoteSocketAddress());

                // kill the conneciton if something is received
                new Thread(() -> {
                    try {
                        s.getInputStream().read();
                    } catch(Exception e) {
                        // don't care
                    }
                    try {
                        s.close();
                    } catch (IOException e) {
                        // don't care
                    }
                }).start();

                PrintWriter pw = new PrintWriter(new OutputStreamWriter(s.getOutputStream()));

                Entry target = new Entry("TARGET", 0, 0);
                while(!s.isClosed()) {
                    Thread.sleep((long) (iat.sample() * 1000 * 60));

                    target.votes = rng.nextLong(votes);
                    int pos = Collections.binarySearch(titles, target);
                    if (pos < 0)
                        pos = -pos;

                    Entry entry = titles.get(pos);
                    int rating = (int) (Math.floor(Math.min(10.0, rng.nextFloat() * entry.rating * 2.0)));
                    pw.println(entry.id + "\t" + rating);
                    pw.flush();
                }
            } catch(Exception e) {
                // don't care
            }
            log.info("connection closed");
        }
    }
}
