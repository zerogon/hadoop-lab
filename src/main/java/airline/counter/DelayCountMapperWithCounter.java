package airline.counter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import common.AirlinePerformanceParser;

/*
[카운터 사용용도]

 맵 함수에서는 입력 데이터를 처리하면서 특정 조건이 충족될 때 카운터 값을 증가시키고,
 리듀스 함수에서는 각 맵 태스크에서 증가된 카운터 값을 합산하여 최종적인 결과를 출력하는 등의 용도로 활용될 수 있습니다.
*/ 
 
public class DelayCountMapperWithCounter
extends Mapper<LongWritable, Text, Text, IntWritable> {
// 작업 구분
private String workType;
// map 출력값
private final static IntWritable outputValue = new IntWritable(1);
// map 출력키
private Text outputKey = new Text();

@Override
public void setup(Context context) throws IOException, InterruptedException {
  workType = context.getConfiguration().get("workType");
}

public void map(LongWritable key, Text value, Context context)
  throws IOException, InterruptedException {

  AirlinePerformanceParser parser = new AirlinePerformanceParser(value);

  // 출발 지연 데이터 출력
  if (workType.equals("departure")) {
    if (parser.isDepartureDelayAvailable()) {
      if (parser.getDepartureDelayTime() > 0) {
        // 출력키 설정
        outputKey.set(parser.getYear() + "," + parser.getMonth());
        // 출력 데이터 생성
        context.write(outputKey, outputValue);
      } else if (parser.getDepartureDelayTime() == 0) {
        context.getCounter(DelayCounters.scheduled_departure).increment(1);
      } else if (parser.getDepartureDelayTime() < 0) {
        context.getCounter(DelayCounters.early_departure).increment(1);
      }
    } else {
      context.getCounter(DelayCounters.not_available_departure).increment(1);
    }
    // 도착 지연 데이터 출력
  } else if (workType.equals("arrival")) {
    if (parser.isArriveDelayAvailable()) {
      if (parser.getArriveDelayTime() > 0) {
        // 출력키 설정
        outputKey.set(parser.getYear() + "," + parser.getMonth());
        // 출력 데이터 생성
        context.write(outputKey, outputValue);
      } else if (parser.getArriveDelayTime() == 0) {
        context.getCounter(
          DelayCounters.scheduled_arrival).increment(1);
      } else if (parser.getArriveDelayTime() < 0) {
        context.getCounter(DelayCounters.early_arrival).increment(1);
      }
    } else {
      context.getCounter(DelayCounters.not_available_arrival).increment(1);
    }
  }
}
}