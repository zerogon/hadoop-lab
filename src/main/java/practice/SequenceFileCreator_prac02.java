package practice;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import common.AirlinePerformanceParser;

public class SequenceFileCreator_prac02 extends Configured implements Tool {

	static class DistanceMapper_prac02 extends MapReduceBase 
		implements Mapper<LongWritable, Text, IntWritable, Text>{
		
		private IntWritable outputKey = new IntWritable();
		
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			
			try {
				AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
				
				if(parser.isDistanceAvailable()) {
					outputKey.set(parser.getDistance());
					output.collect(outputKey, value);
				}
			} catch (ArrayIndexOutOfBoundsException ae) {
				outputKey.set(0);
				output.collect(outputKey, value);
				ae.printStackTrace();
			} catch(Exception e) {
				outputKey.set(0);
				output.collect(outputKey, value);
				e.printStackTrace();
			}
		}
	}
	public int run(String[] arg) throws Exception {
		
		JobConf conf = new JobConf(SequenceFileCreator_prac02.class);
		
		conf.setJobName("seqfile");
		
		FileInputFormat.setInputPaths(conf, new Path(arg[0]));
		FileOutputFormat.setOutputPath(conf, new Path(arg[1]));
		
		conf.setMapperClass(DistanceMapper_prac02.class);
		conf.setNumReduceTasks(0);
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		SequenceFileOutputFormat.setCompressOutput(conf, true);
		SequenceFileOutputFormat.setOutputCompressionType(conf, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
		
		JobClient.runJob(conf);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new SequenceFileCreator_prac02(), args);
	}
	

}
