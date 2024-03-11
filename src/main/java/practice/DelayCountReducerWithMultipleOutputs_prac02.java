package practice;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class DelayCountReducerWithMultipleOutputs_prac02 extends Reducer<Text, IntWritable, Text, IntWritable> {

	private MultipleOutputs<Text, IntWritable> mos ;

	private Text outputKey = new Text();
	private IntWritable outputValue = new IntWritable();
	
	
	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {
		
		String [] cols = arg0.toString().split(",");
		
		outputKey.set(cols[1]+","+cols[2]);
		
		if(cols[0].equals("d")) {
			int sum  = 0;
			for(IntWritable val : arg1) {
				sum += val.get();
			}
			outputValue.set(sum);
			
			mos.write("d", outputKey, outputValue);
			
			
		}else {
			
		}
		
	}

	@Override
	protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		mos = new MultipleOutputs<Text, IntWritable>(context);
	}
	
	
	
	

}
