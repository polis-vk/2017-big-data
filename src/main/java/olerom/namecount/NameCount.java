package olerom.namecount;

import olerom.wordcount.WordCountReducer;
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
 * Date: 30.10.17
 *
 * @author olerom
 */
public class NameCount extends Configured implements Tool {
    public int run(final String[] strings) throws Exception {
        final Job job = new Job(getConf(), "olerom.namecount.NameCount");
        job.setJarByClass(getClass());

        TextInputFormat.addInputPath(job, new Path(strings[0]));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(NameCountMapper.class);
        job.setReducerClass(NameCountReducer.class);

        TextOutputFormat.setOutputPath(job, new Path(strings[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new NameCount(), args);
        System.exit(exitCode);
    }
}