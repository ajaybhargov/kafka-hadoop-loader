package co.gridport.kafka.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class KafkaInputFormat extends InputFormat<LongWritable, BytesWritable> {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        System.out.println("getSplits method called");
        Configuration conf = context.getConfiguration();
        ZkUtils zk = new ZkUtils(conf.get("kafka.zk.connect"), conf.getInt("kafka.zk.sessiontimeout.ms", 10000),
            conf.getInt("kafka.zk.connectiontimeout.ms", 10000));
        String[] inputTopics = conf.get("kafka.topics").split(",");
        System.out.println("inputTopics = " + Arrays.toString(inputTopics));
        String consumerGroup = conf.get("kafka.groupid");
        List<InputSplit> splits = new ArrayList<InputSplit>();
        for (String topic : inputTopics) {
            List<String> brokerPartitions = zk.getBrokerPartitions(topic);
            System.out.println("brokerPartitions = " + brokerPartitions);
            for (String partition : brokerPartitions) {
                String[] brokerPartitionParts = partition.split("-");
                String brokerId = brokerPartitionParts[0];
                String partitionId = brokerPartitionParts[1];
                long lastConsumedOffset = zk.getLastConsumedOffset(consumerGroup, topic, partition);
                KafkaInputSplit split =
                    new KafkaInputSplit(brokerId, "localhost:9092", topic, Integer.valueOf(partitionId), lastConsumedOffset);
                System.out.println("split = " + split);
                splits.add(split);
            }
        }
        zk.close();
        System.out.println("splits = " + splits);
        return splits;
    }

    @Override
    public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit arg0, TaskAttemptContext arg1)
        throws IOException, InterruptedException {
        return new KafkaInputRecordReader();
    }

}
