package mburmistrov.task2;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCertainPosition extends Configured implements Tool {
  private static int vNum;

  private static IntWritable ONE = new IntWritable(1);

  static class WordCertainPositionMapper extends Mapper<Object, Text, IntWritable, TextWCount> {
    private int count;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      count = 0;
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      final String line = value.toString();

      int pos = line.indexOf(0x09);

      if (pos >= 0) {
        int inputCount = Integer.valueOf(line.substring(0, pos));
        String inputString = line.substring(pos + 1);

        if (count <= vNum) {
          context.write(ONE, new TextWCount(inputString, inputCount));
          count++;
        }
      }
    }
  }

  static class WordCertainPositionReducer extends Reducer<IntWritable, TextWCount, Text, IntWritable> {
    private SortedSet<TextWCount> setOfTextWithCount;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      setOfTextWithCount = new TreeSet<>();
    }

    @Override
    protected void reduce(IntWritable key, Iterable<TextWCount> values, Context context) throws IOException, InterruptedException {
      values.forEach((textWCount -> setOfTextWithCount.add(textWCount.clone())));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);

      int i = 0;

      for (TextWCount textWCount : setOfTextWithCount) {
        if (i == vNum) {
          context.write(new Text(textWCount.getText()), new IntWritable(textWCount.getCount()));
          break;
        }
        i++;
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    final Configuration conf = this.getConf();
    final Job job = new Job(conf, "WordCertainPosition");

    vNum = Integer.valueOf(args[2]);

    job.setJarByClass(WordCertainPosition.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(WordCertainPositionMapper.class);
    job.setReducerClass(WordCertainPositionReducer.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(TextWCount.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    final int returnCode = ToolRunner.run(new Configuration(), new WordCertainPosition(), args);

    System.exit(returnCode);
  }
}
