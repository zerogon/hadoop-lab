package practice;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import sort.DateKey;

public class GroupKeyComparator_prac extends WritableComparator {

	protected GroupKeyComparator_prac() {
		super(DateKey.class, true);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable a, WritableComparable b) {
		DateKey k1 = (DateKey) a ;
		DateKey k2 = (DateKey) b ;
		
		return k1.getYear().compareTo(k2.getYear());
	}
	
	

}
