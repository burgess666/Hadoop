package MapReduce;

/*sortMapper.java*/
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class sortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        /*eliminate leading and trailing whitespace, then split the string by matching any whitespaces*/

        String[] splits = value.toString().trim().split("\\s+");
/*put page count number as Key and project name as value for output*/

        context.write(new LongWritable(Long.parseLong(splits[1])), new Text(splits[0]));
    }
}
