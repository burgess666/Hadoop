package MapReduce;

/*SumReducer.java*/
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        /*add up all values in Mapper output list*/
        long wordCount = 0;
        for (LongWritable value : values) {
            wordCount += value.get();
        }
        /*write final output*/
        context.write(key, new LongWritable(wordCount));
    }

}
