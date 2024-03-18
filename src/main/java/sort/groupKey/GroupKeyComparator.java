package sort.groupKey;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import sort.DateKey;

public class GroupKeyComparator extends WritableComparator {

	  protected GroupKeyComparator() {
	    super(DateKey.class, true);
	  }

	  @SuppressWarnings("rawtypes")
	  @Override
	  public int compare(WritableComparable w1, WritableComparable w2) {
	    DateKey k1 = (DateKey) w1;
	    DateKey k2 = (DateKey) w2;

	    //연도값 비교
	    return k1.getYear().compareTo(k2.getYear());
	  }
	}