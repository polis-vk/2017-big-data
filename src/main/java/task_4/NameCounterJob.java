package task_4;

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
 * Created by iters on 11/13/17.
 */
public class NameCounterJob extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = new Job(getConf(), "NameCount");
        job.setJarByClass(getClass());

        TextInputFormat.addInputPath(job,
                new Path(strings[0]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(WordCounterMapper.class);
        job.setReducerClass(NameCounterReducer.class);

        TextOutputFormat.setOutputPath(job,
                new Path(strings[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        try {
            int exitCode = ToolRunner.run(new NameCounterJob(), args);
            System.exit(exitCode);
        } catch (Exception e) {
            System.out.println("error on start ;(");
            e.printStackTrace();
        }
    }
}
