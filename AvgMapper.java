package MapReduce;

/*AvgMapper.java*/
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public class AvgMapper extends Mapper <LongWritable, Text, Text, LongWritable> {
    public void map(LongWritable Key, Text value, Context context)
            throws IOException, InterruptedException {
        /*split line by matching any whitespace*/
        String[] map_split = value.toString().split("\\s+");
  /*check any line not contains two element*/
        if (map_split.length == 2) {
/*split first column by matching any non-word characters to get language code*/

            String[] language_name = map_split[0].split("\\s+");
            /*output language code as key and page count number as value*/

            context.write(new Text(language_name[0]), new LongWritable(Long.parseLong(map_split[1])));
        }
    }
}

