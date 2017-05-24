package MapReduce;

/*AvgReducer.java*/
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class AvgReducer extends Reducer<Text, LongWritable, Text, DoubleWritable> {
    /*define a Final Text type variable to get command line argument
    private final static Text time_period = new Text();

    @Override
/*get command line argument via configuration*/
    public void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String gettime = conf.get("hours");
        time_period.set(gettime);
    }

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        long temp = 0;

        double hours = Double.parseDouble(time_period.toString());
        /*add up all values*/  
 for (LongWritable val : values) {
            temp += val.get();
        }
        double sum = temp;
        double avg = sum / hours;
        context.write(key, new DoubleWritable(avg));

    }
}
