package practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapFileCreator_prac extends Configured implements Tool {

	public int run(String[] arg0) throws Exception {
		
		JobConf conf = new JobConf(MapFileCreator_prac.class);
		
		conf.setJobName("dd");
		
		FileInputFormat.setInputPaths(conf, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(conf, new Path(arg0[1]));
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(MapFileOutputFormat.class);
		
		conf.setOutputKeyClass(IntWritable.class);
		
		SequenceFileOutputFormat.setCompressOutput(conf, true);
		SequenceFileOutputFormat.setOutputCompressionType(conf, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
		
		JobClient.runJob(conf);
		
		
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MapFileCreator_prac(), args);
	}

}
