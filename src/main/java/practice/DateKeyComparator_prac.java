package practice;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import sort.DateKey;

public class DateKeyComparator_prac extends WritableComparator{

	protected DateKeyComparator_prac() {
		super(DateKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		
		DateKey k1 = (DateKey) w1;  
		DateKey k2 = (DateKey) w2;
		
		// 연도 비교
		int result = k1.getYear().compareTo(k2.getYear());
		if(result != 0) {
			return result;
		}
		
		
		return k1.getMonth() == k2.getMonth() ? 0 : (k1.getMonth() < k2.getMonth() ? -1 : 1);
	}
	
	
	
}
