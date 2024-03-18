package sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class DateKey implements WritableComparable<DateKey> {

	  private String year;
	  private Integer month;

	  public DateKey() {
	  }

	  public DateKey(String year, Integer date) {
	    this.year = year;
	    this.month = date;
	  }

	  public String getYear() {
	    return year;
	  }

	  public void setYear(String year) {
	    this.year = year;
	  }

	  public Integer getMonth() {
	    return month;
	  }

	  public void setMonth(Integer month) {
	    this.month = month;
	  }

	  @Override
	  public String toString() {
	    return (new StringBuilder()).append(year).append(",").append(month)
	      .toString();
	  }

	  /*
	   * DataInput 인터페이스에는 문자열을 바로 읽어오는 메서드가 없습니다. 
	   * 따라서 문자열을 읽어오기 위해서는 직접 문자열의 길이 정보를 읽어들인 후에 해당 길이만큼 바이트 배열로 읽어와서 문자열로 변환해야 합니다.
	   * 
	   * WritableUtils.readString
	   * 이 메서드는 문자열의 길이를 먼저 읽어들인 후에 해당 길이만큼 바이트 배열로 읽어와서 문자열로 변환합니다
	   */
	  public void readFields(DataInput in) throws IOException {
	    year = WritableUtils.readString(in);
	    month = in.readInt();
	  }

	  public void write(DataOutput out) throws IOException {
	    WritableUtils.writeString(out, year);
	    out.writeInt(month);
	  }
	  // 0인경우 같은값
	  // -1 인경우 뒤에가 더 큼, 사전순으로 a보다 b가 큼, 숫자 오름차순
	  public int compareTo(DateKey key) {
	    int result = year.compareTo(key.year);
	    if (0 == result) {
	      result = month.compareTo(key.month);
	    }
	    return result;
	  }

	}