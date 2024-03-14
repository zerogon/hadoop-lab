package practice;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import sort.DateKey;
public class DateKeyComparator_prac02 extends WritableComparator {

	protected DateKeyComparator_prac02() {
		super(DateKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		DateKey k1 = (DateKey) a;
		DateKey k2 = (DateKey) b;
		
		int res = k1.getYear().compareTo(k2.getYear());
		if(res != 0) return res;
		
		return k1.getMonth() == k2.getMonth() ? 0 : (k1.getMonth() < k2.getMonth() ? -1 : 1);
	}
	
	

}
