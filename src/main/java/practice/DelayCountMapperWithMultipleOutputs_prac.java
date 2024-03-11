package practice;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import airline.counter.DelayCounters;
import common.AirlinePerformanceParser;

public class DelayCountMapperWithMultipleOutputs_prac extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private static IntWritable outputValue = new IntWritable(1);
	private Text outputKey = new Text();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		outputKey.set(parser.getYear()+"a");
		context.write(outputKey, outputValue);
		context.getCounter(DelayCounters.early_arrival).increment(1);
		
	}
	
	
	
	
	
	

}
