package practice;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DelayCountReducer_prac extends Reducer<Text, IntWritable, Text, IntWritable> {

	
	private IntWritable result = new IntWritable();
	@Override
	protected void reduce(Text word, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(word, result);
		
	}
	
}
