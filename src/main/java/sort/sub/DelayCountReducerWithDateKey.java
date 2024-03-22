package sort.sub;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import sort.DateKey;
/*
GroupKeyPartitioner는 하둡에서 사용되는 커스텀 파티셔너의 한 종류입니다. 
이를 사용하는 이유는 데이터를 여러 개의 리듀서로 분할할 때, 
특정 키를 기준으로 데이터를 그룹화하여 동일한 키를 가진 데이터가 동일한 리듀서로 보내도록 하는 것입니다.
 */

public class DelayCountReducerWithDateKey extends
  Reducer<DateKey, IntWritable, DateKey, IntWritable> {

  private MultipleOutputs<DateKey, IntWritable> mos;

  // reduce 출력키
  private DateKey outputKey = new DateKey();

  // reduce 출력값
  private IntWritable result = new IntWritable();

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    mos = new MultipleOutputs<DateKey, IntWritable>(context);
  }

  public void reduce(DateKey key, Iterable<IntWritable> values,
                     Context context) throws IOException, InterruptedException {
    // 콤마 구분자 분리
    String[] colums = key.getYear().split(",");

    int sum = 0;
    Integer bMonth = key.getMonth();

    if (colums[0].equals("D")) {
      for (IntWritable value : values) {
        if (bMonth != key.getMonth()) {
          result.set(sum);
          outputKey.setYear(key.getYear().substring(2));
          outputKey.setMonth(bMonth);
          mos.write("departure", outputKey, result);
          sum = 0;
        }
        sum += value.get();
        bMonth = key.getMonth();
      }
      if (key.getMonth() == bMonth) {
        outputKey.setYear(key.getYear().substring(2));
        outputKey.setMonth(key.getMonth());
        result.set(sum);
        mos.write("departure", outputKey, result);
      }
    } else {
      for (IntWritable value : values) {
        if (bMonth != key.getMonth()) {
          result.set(sum);
          outputKey.setYear(key.getYear().substring(2));
          outputKey.setMonth(bMonth);
          mos.write("arrival", outputKey, result);
          sum = 0;
        }
        sum += value.get();
        bMonth = key.getMonth();
      }
      if (key.getMonth() == bMonth) {
        outputKey.setYear(key.getYear().substring(2));
        outputKey.setMonth(key.getMonth());
        result.set(sum);
        mos.write("arrival", outputKey, result);
      }
    }
  }

  @Override
  public void cleanup(Context context) throws IOException,
    InterruptedException {
    mos.close();
  }
}