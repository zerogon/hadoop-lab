package practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SearchValueList_prac extends Configured implements Tool{

	public int run(String[] arg0) throws Exception {
		
		Path path = new Path(arg0[0]);
 		FileSystem fs = path.getFileSystem(getConf());
 		
 		
 		Reader[] readers = MapFileOutputFormat.getReaders(fs, path, getConf());
 		
 		IntWritable key = new IntWritable();
 		key.set(Integer.parseInt(arg0[1]));
 		
 		Text value = new Text();
 		
 		Partitioner<IntWritable, Text> partitioner = new HashPartitioner<IntWritable, Text>();
 		Reader reader = readers[partitioner.getPartition(key, value, readers.length)];
 		
 		Writable entry = reader.get(key, value);
 		
 		if(entry == null) {
 			System.out.println("key was not found");
 		}
 		
 		IntWritable nextKey = new IntWritable();
 		
 		do {
 			System.out.println(value.toString());
 			
 		}while(reader.next(nextKey, value)&& key.equals(nextKey));
 		
 		
 		
 		
 		
		
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new SearchValueList_prac(), args);
	}

}
