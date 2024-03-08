package practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class DelayCount_prac02 extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DelayCount_prac02(), args);
		System.out.println("res"+ res);
	}
	
	public int run(String[] arg0) throws Exception {
		
		String[] otherArgs = new GenericOptionsParser(getConf(), arg0).getRemainingArgs();
		
		if(otherArgs.length != 2 ) {
			System.out.println("user 1,2");
		}
		
		
		Job job = new Job(getConf(), "delay");
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.setJarByClass(DelayCount_prac02.class);
		job.setMapperClass(DelayCountMapper_prac02.class);
		job.setReducerClass(DelayCountReducer_prac.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		job.waitForCompletion(true);
		
		
		
		
		return 0;
	}

}
