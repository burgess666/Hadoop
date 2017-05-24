package MapReduce;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import java.net.URI;

public class WikiPage extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
       /* implement of toolrunner interface*/
int res = ToolRunner.run(new Configuration(), new WikiPage(), args);
        System.exit(res);

    }

    @Override
    public int run(String[] args) throws Exception {

/*checks command line arguments*/
        if (args.length != 5) {
            System.err.println("Usage: <input> <temp1> <task1> <temp2> <task2>");
            System.exit(-1);
        }

            /*set time period parameter, pass it to reducer class by configuration*/
        Configuration conf = this.getConf();

                  /*start first job, count total page view numbers in the input path*/
        Job PageCount = Job.getInstance();
        PageCount.setJarByClass(WikiPage.class);
        PageCount.setJobName("PageCount");
        PageCount.setMapperClass(PageMapper.class);
        PageCount.setReducerClass(SumReducer.class);
        PageCount.setOutputKeyClass(Text.class);
        PageCount.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(PageCount, new Path(args[0]));
        FileOutputFormat.setOutputPath(PageCount, new Path(args[1]));

        PageCount.waitForCompletion(true);

                 /*start second job, sort the output from first job to descending order*/
    Job sortPage = Job.getInstance();
        sortPage.setJarByClass(WikiPage.class);
        sortPage.setJobName("sortPage");
        sortPage.setMapperClass(sortMapper.class);
        sortPage.setReducerClass(sortReducer.class);
        sortPage.setSortComparatorClass(sortFunc.class);
/*set Mapper output key class to fit Comparator class*/
        sortPage.setMapOutputKeyClass(LongWritable.class);
        sortPage.setMapOutputValueClass(Text.class);
        sortPage.setOutputKeyClass(Text.class);
        sortPage.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(sortPage, new Path(args[1]));
        FileOutputFormat.setOutputPath(sortPage, new Path(args[2]));

        sortPage.waitForCompletion(true);

/*start third job, calculate average page count per language*/
            /*time period parameters pass to this reducer class*/
        Job PageAvg = Job.getInstance(conf);
        PageAvg.setJarByClass(WikiPage.class);
        PageAvg.setJobName("PageAvg");
        PageAvg.setMapperClass(AvgMapper.class);
        PageAvg.setReducerClass(AvgReducer.class);

        PageAvg.setMapOutputKeyClass(Text.class);
        PageAvg.setMapOutputValueClass(LongWritable.class);

        PageAvg.setOutputKeyClass(Text.class);
        PageAvg.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(PageAvg, new Path(args[1]));
        FileOutputFormat.setOutputPath(PageAvg, new Path(args[3]));

        PageAvg.waitForCompletion(true);
/*last job, sort the output from above job to descending order*/

        Job sortPage2 = Job.getInstance();
        sortPage2.setJarByClass(WikiPage.class);
        sortPage2.setJobName("sortPage2");
        sortPage2.setMapperClass(sortMapper2.class);
        sortPage2.setReducerClass(sortReducer2.class);
        sortPage2.setSortComparatorClass(sortFunc2.class);

        sortPage2.setMapOutputKeyClass(DoubleWritable.class);
        sortPage2.setMapOutputValueClass(Text.class);

        sortPage2.setOutputKeyClass(Text.class);
        sortPage2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(sortPage2, new Path(args[3]));
        FileOutputFormat.setOutputPath(sortPage2, new Path(args[4]));

        sortPage2.waitForCompletion(true);

        return 0;
    }

}


