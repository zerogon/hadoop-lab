package practice;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import common.AirlinePerformanceParser;

public class DepartureDelayCountMapper_prac extends Mapper<LongWritable, Text, Text, IntWritable>{

	private Text outputKey = new Text();
	private static IntWritable one = new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		outputKey.set(parser.getYear()+","+parser.getMonth());
		
		if(parser.getDepartureDelayTime() > 0 ) {
			context.write(outputKey, one);
		}
		
	}

	
}
