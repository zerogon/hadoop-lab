package practice;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import common.AirlinePerformanceParser;

public class DelayCountMapper_prac extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private String worktype = "";
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		worktype = context.getConfiguration().get("worktype");
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		if(parser.getArriveDelayTime() > 0 && worktype.equals("Arrive")) {
			contextWrite(parser, context, word, one);
		}else if (parser.getDepartureDelayTime() > 0 && worktype.equals("Departure")){
			contextWrite(parser, context, word, one);
		}
	}
	
	protected void contextWrite(AirlinePerformanceParser parser, Context context, Text ouputKey, IntWritable ouputValue) throws IOException, InterruptedException {
		word.set(parser.getYear()+","+parser.getMonth());
		context.write(ouputKey, ouputValue);
	}
	
	
}
