package task_2;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by iters on 10/21/17.
 */

public class DescendingSorting extends Configured implements Tool {
    public int run(String[] strings) throws Exception {
        Job job = new Job(getConf(), "DescendingSortingWords");
        job.setJarByClass(getClass());

        TextInputFormat.addInputPath(job,
                new Path(strings[0]));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(WordCounterMapper.class);
        job.setReducerClass(WordCounterReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        TextOutputFormat.setOutputPath(job,
                new Path(strings[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        try {
            int exitCode = ToolRunner.run(new DescendingSorting(), args);
            System.exit(exitCode);
        } catch (Exception e) {
            System.out.println("error on start ;(");
            e.printStackTrace();
        }
    }
}