package task_3;

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

/**
 * Created by iters on 10/21/17.
 */
public class StopWordProportion extends Configured implements Tool {

    enum stopWordProportion {
        STOP_WORD,
        COMMON_WORD
    }

    public int run(String[] strings) throws Exception {
        Job job = new Job(getConf(), "StopWordProportion");
        job.setJarByClass(getClass());

        TextInputFormat.addInputPath(job, new Path(strings[0]));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(WordCounterMapper.class);
        // job.setReducerClass(task_1.WordCounterReducer.class);
        job.setNumReduceTasks(0);

        TextOutputFormat.setOutputPath(job, new Path(strings[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        int exitCode =  job.waitForCompletion(true) ? 0 : 1;

        Counters counters = job.getCounters();
        long stopWords = counters.findCounter(stopWordProportion.STOP_WORD).getValue();
        long commonWords = counters.findCounter(stopWordProportion.COMMON_WORD).getValue();
        double res = Double.parseDouble(stopWords + "") /
                (commonWords + stopWords) * 100d;

        System.out.println(res + "% стоп-слов в en_articles");
        return exitCode;
    }

    public static void main(String[] args) {
        try {
            int exitCode = ToolRunner.run(new StopWordProportion(), args);
            System.exit(exitCode);
        } catch (Exception e) {
            System.out.println("error on start ;(");
            e.printStackTrace();
        }
    }
}