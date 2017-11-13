package olerom.stopwordcount;

import olerom.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Date: 12.11.17
 *
 * @author olerom
 */
public class StopWordCount extends Configured implements Tool {
    public int run(final String[] strings) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("stopWords", strings[2]);

        final Job job = new Job(configuration, "Stop words");
//        final Job job = new Job(getConf(), "Stop words");

        job.setJarByClass(getClass());

        TextInputFormat.addInputPath(job, new Path(strings[0]));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(StopWordCountMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(strings[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        int exit = job.waitForCompletion(true) ? 0 : 1;
        Counters counter = job.getCounters();

        System.out.println("Stop words: " + counter.findCounter(StopWordCountMapper.MATCH_COUNTER.STOP_WORD).getValue());
        System.out.println("Total: " + counter.findCounter(StopWordCountMapper.MATCH_COUNTER.TOTAL).getValue());
        System.out.println("Percentage: "
                + ((double) counter.findCounter(StopWordCountMapper.MATCH_COUNTER.STOP_WORD).getValue()
                / counter.findCounter(StopWordCountMapper.MATCH_COUNTER.TOTAL).getValue())
        );

        return exit;
    }

    public static void main(String[] args) throws Exception {
        final int exitCode = ToolRunner.run(new StopWordCount(), args);
        System.exit(exitCode);
    }
}
