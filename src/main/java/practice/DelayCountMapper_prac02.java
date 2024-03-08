package practice;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import common.AirlinePerformanceParser;

public class DelayCountMapper_prac02 extends Mapper<LongWritable, Text, Text, IntWritable>{
	private String workType = "";
	private Text outputKey = new Text();
	private static IntWritable outputValue = new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		if("Arrive".equals(workType)) {
			outputKey.set(parser.getYear()+","+parser.getMonth());
			context.write(outputKey, outputValue);
		}
		
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		workType = context.getConfiguration().get("worktype");
	}
	
	

}
