package practice;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import common.AirlinePerformanceParser;
import common.CarrierCodeParser;

public class MapperWithMapSideJoin_prac extends Mapper<LongWritable, Text, Text, Text>{

	private Hashtable<String, String> joinMap = new Hashtable<String, String>();
	private Text outputKey = new Text();
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		try {
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if(cacheFiles != null && cacheFiles.length > 0) {
				String line ;
				BufferedReader br = new BufferedReader(new FileReader(cacheFiles[0].toString()));
				try {
					while((line = br.readLine()) != null) {
						CarrierCodeParser parser = new CarrierCodeParser(line);
						joinMap.put(parser.getCarrierCode(), parser.getCarrierName());
					}
				}finally {
					br.close();
				}
			}else {
				System.out.println("cacheFiles is null");
			}
			
		} catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
			
		
	}
	
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		outputKey.set(parser.getUniqueCarrier());
		context.write(outputKey, new Text(joinMap.get(parser.getUniqueCarrier())+"\t"+value.toString()));
	}

	
	

}
