package MapReduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
/*Mapper takes each line of a file, it split the line by matching any whitespace*/

        String[] splits = value.toString().split("\\s+");

        if (splits.length == 4) {
/*output is the first column of the line and third column of the line*/

            context.write(new Text(splits[0]), new LongWritable(Long.parseLong(splits[2])));
        }
    }
}

