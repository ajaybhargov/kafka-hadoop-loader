package co.gridport.kafka.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class FakeHadoopJob {

    static protected long grandTotal = 0;

    static protected Object lock = new Object();

    public static void main(String[] args) throws InterruptedException, IOException {
        //Mock the hadoop job with executor of MockMapTask pool
        ExecutorService pool = Executors.newCachedThreadPool();

        JobConf conf = new JobConf(new Configuration());
        //TODO extract options from HadoopJob and reuse here
        conf.set("kafka.zk.connect",
            "zookeeper-01.stag.visualdna.com:2181,zookeeper-02.stag.visualdna.com:2181,zookeeper-03.stag.visualdna.com:2181");
        conf.set("kafka.zk.sessiontimeout.ms", "10000");
        conf.set("kafka.zk.connectiontimeout.ms", "10000");
        conf.set("kafka.topics", "prod_useractivity,prod_profileupdates,prod_conversions,prod_pageviews,prod_adclicks,prod_adviews");
        conf.set("kafka.groupid", "hadoop-loader-test");
        //conf.set("kafka.watermark.reset", "earliest");
        conf.set("input.format", "json");

        JobContext dummyJobContext = new Job(conf);

        KafkaInputFormat realInputFormat = new KafkaInputFormat();
        List<InputSplit> splits = realInputFormat.getSplits(dummyJobContext);

        Collection<Callable<String>> tasks = new ArrayList<Callable<String>>();
        for (InputSplit split : splits) {
            tasks.add(new MockMapTask(split, conf));
        }

        for (Future<String> future : pool.invokeAll(tasks)) {
            try {
                System.out.println(future.get());
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        System.out.println();
        System.out.println("GRAND TOTAL = " + grandTotal);

        pool.shutdown();

    }

    public static class MockMapTask implements Callable<String> {
        private KafkaInputSplit split;
        private KafkaInputRecordReader realRecordReader;
        private String info;
        private HadoopJobMapper mapper;
        private Configuration conf;

        public MockMapTask(InputSplit split, Configuration conf) throws IOException, InterruptedException {
            this.mapper = new HadoopJobMapper();
            this.conf = conf;
            this.split = (KafkaInputSplit) split;
            info = this.split.getTopic() + "/" + this.split.getBrokerHost() + ":" + this.split.getPartition();
            realRecordReader = new KafkaInputRecordReader();
            realRecordReader.initialize(split, conf);
        }

        public String call() {
            Boolean success = false;
            int count = 0;
            try {
                while (realRecordReader.nextKeyValue()) {
                    //realRecordReader.getProgress
                    LongWritable key = realRecordReader.getCurrentKey();
                    BytesWritable value = realRecordReader.getCurrentValue();
                    if (mapper.map(key, value, conf) != null) {
                        count++;
                    }
                }
                realRecordReader.close();
                success = true;
            } catch (Exception e) {
                e.printStackTrace();
                success = false;
            }
            grandTotal += count;
            return (success ? "SUCCESS" : "FAILURE") + " Processed " + info + " count " + count;
        }
    }
}
