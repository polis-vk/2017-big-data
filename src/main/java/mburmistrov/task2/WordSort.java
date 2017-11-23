package mburmistrov.task2;

import mburmistrov.task1.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class WordSort extends Configured implements Tool {
  static class Comparator extends WritableComparator {
    protected Comparator() {
      super(IntWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return -Integer.compare(readInt(b1, s1), readInt(b2, s2));
    }
  }

  @Override
  public int run(final String[] args) throws Exception {
    final Configuration conf = this.getConf();
    final Job job = Job.getInstance(conf, "Word Sort");
    job.setJarByClass(WordSort.class);

    job.setMapperClass(WordSortMapper.class);
    job.setReducerClass(WordSortReducer.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setSortComparatorClass(Comparator.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(final String[] args) throws Exception {
    final int returnCode = ToolRunner.run(new Configuration(), new WordCount(), args);
    System.exit(returnCode);
  }
}
