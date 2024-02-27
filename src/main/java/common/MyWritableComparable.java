package common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


/* 108
 * 
 * 맵리듀스 프로그램에서 키와 값으로 사용되는 모든 데이터 타입은 반드시 WritableComparable 구현필요.
 * 직접 인터페이스를 이용해 데이터 타입 개발 가능.
 */

public class MyWritableComparable implements WritableComparable<Object> {
	  private int counter;
	  private long timestamp;

	  public void write(DataOutput out) throws IOException {
	    out.writeInt(counter);
	    out.writeLong(timestamp);
	  }

	  public void readFields(DataInput in) throws IOException {
	    counter = in.readInt();
	    timestamp = in.readLong();
	  }

	  public int compareTo(Object o) {
	    MyWritableComparable w = (MyWritableComparable)o;
	    if(counter > w.counter) {
	      return -1;
	    } else if(counter < w.counter) {
	      return 1;
	    } else {
	      if(timestamp < w.timestamp) {
	        return 1;
	      } else if(timestamp > w.timestamp) {
	        return -1;
	      } else {
	        return 0;
	      }
	    }
	  }
	}