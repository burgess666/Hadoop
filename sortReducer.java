package MapReduce;

/*sortReducer.java*/
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class sortReducer extends Reducer<LongWritable, Text, Text, LongWritable>{
    @Override
    /*same as WordCount example, just output the list of values from Mapper as key, and key from Mapper output as value  */
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text val: values) {
            context.write(val, key);
        }
    }
}
