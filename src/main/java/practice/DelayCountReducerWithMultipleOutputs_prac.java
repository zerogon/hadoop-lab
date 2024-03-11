package practice;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class DelayCountReducerWithMultipleOutputs_prac extends Reducer<Text, IntWritable, Text, IntWritable> {

	
	private MultipleOutputs<Text, IntWritable> mos ;
	
	private IntWritable outputValue = new IntWritable(); // check
	private Text outputKey = new Text();
	
	
	@Override
	protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		mos = new MultipleOutputs<Text, IntWritable>(context);
	}



	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			
		String [] colums = outputKey.toString().split(",");
		
		if(colums[0].equals("D")) {
			int sum = 0;
			for(IntWritable val : values) {
				sum += val.get();
			}
			outputKey.set(colums[1]+","+colums[2]);
			outputValue.set(sum);
			
			mos.write("D", outputKey, outputValue);
			
		
		}else if(colums[0].equals("A")) {
			
		}
		
	}
	
	

}
