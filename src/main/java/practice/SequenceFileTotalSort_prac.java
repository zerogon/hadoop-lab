package practice;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SequenceFileTotalSort_prac extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new SequenceFileTotalSort_prac(), args);
	}
	public int run(String[] arg0) throws Exception {
		
		JobConf conf = new JobConf(getConf(),SequenceFileTotalSort_prac.class);
		
		conf.setJobName("test");
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(TotalOrderPartitioner.class);
		
		SequenceFileOutputFormat.setOutputCompressionType(conf, CompressionType.BLOCK);
		SequenceFileOutputFormat.setCompressOutput(conf, true);
		SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
		
		FileInputFormat.setInputPaths(conf, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(conf, new Path(arg0[1]));
		
		Path inputDir = FileInputFormat.getInputPaths(conf)[0];
		inputDir = inputDir.makeQualified(inputDir.getFileSystem(conf));
		Path partitionFile = new Path(inputDir, "_partitions");
		TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
		
		
		InputSampler.Sampler<IntWritable, Text> sample = new InputSampler.RandomSampler<IntWritable, Text>(
				0.1, 1000, 10);
		InputSampler.writePartitionFile(conf, sample);
		
		URI partitionUri = new URI(partitionFile.toString()+"#_partition");
		DistributedCache.addCacheFile(partitionUri, conf);
		DistributedCache.createSymlink(conf);
		
		JobClient.runJob(conf);
		
		
		
		
		
		
		
		
		
		
		return 0;
	}

}
