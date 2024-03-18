package sort.groupKey;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import sort.DateKey;

public class GroupKeyPartitioner extends Partitioner<DateKey, IntWritable> {

	  @Override
	  public int getPartition(DateKey key, IntWritable val, int numPartitions) {
	    int hash = key.getYear().hashCode();
	    int partition = hash % numPartitions;
	    return partition;
	  }
	}