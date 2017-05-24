package MapReduce;

/*sortFunc.java*/
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class sortFunc extends WritableComparator {
    protected sortFunc() {
        super(LongWritable.class, true);
    }

    @Override

    /*Custom comparator to return negative value of comparision result*/
    /*normal comparator returns positive value that is for ascending order*/

    public int compare(WritableComparable val1, WritableComparable val2) {
        LongWritable key1 = (LongWritable) val1;
        LongWritable key2 = (LongWritable) val2;
        int sortvalue = key1.compareTo(key2);
        return -1*sortvalue;
    }

}

