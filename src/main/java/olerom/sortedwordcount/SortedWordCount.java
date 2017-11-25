package olerom.sortedwordcount;

import olerom.wordcount.WordCount;
import olerom.wordcount.WordCountMapper;
import olerom.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
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
public class SortedWordCount extends Configured implements Tool {
    public int run(final String[] strings) throws Exception {
        final Job firstJob = new Job(getConf(), "Default WC");
        firstJob.setJarByClass(getClass());

        TextInputFormat.addInputPath(firstJob, new Path(strings[0]));
        firstJob.setInputFormatClass(TextInputFormat.class);

        firstJob.setMapperClass(WordCountMapper.class);
        firstJob.setReducerClass(WordCountReducer.class);

        TextOutputFormat.setOutputPath(firstJob, new Path(strings[1]));
        firstJob.setOutputFormatClass(TextOutputFormat.class);
        firstJob.setOutputKeyClass(Text.class);
        firstJob.setOutputValueClass(IntWritable.class);
// Second job
        JobConf secondJobConf = new JobConf(WordCount.class);
        secondJobConf.setJobName("Sorted WC");

//        secondJobConf.setOutputKeyClass(Text.class);
//        secondJobConf.setOutputValueClass(IntWritable.class);
        secondJobConf.setOutputKeyClass(IntWritable.class);
        secondJobConf.setOutputValueClass(Text.class);

        secondJobConf.setMapperClass(SortedWordCountMapper.class);
        secondJobConf.setReducerClass(SortedWordCountReducer.class);

        secondJobConf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
        secondJobConf.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);

        System.out.println(strings[1] + "/part-00000");
        org.apache.hadoop.mapred.FileInputFormat.setInputPaths(secondJobConf, new Path(strings[1] + "/part-r-00000"));
        org.apache.hadoop.mapred.FileOutputFormat.setOutputPath(secondJobConf, new Path(strings[1] + "/result"));

        final Job secondJob = new Job(secondJobConf);

        firstJob.submit();
        if (firstJob.waitForCompletion(true)) {
            secondJob.submit();
            secondJob.waitForCompletion(true);
            return 0;
        }
        return 1;
    }

    public static void main(String[] args) throws Exception {
        final int exitCode = ToolRunner.run(new SortedWordCount(), args);
        System.exit(exitCode);
    }
}