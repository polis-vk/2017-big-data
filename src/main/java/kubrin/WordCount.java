package kubrin;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

public class WordCount extends Configured implements Tool {

  public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private final transient Text word = new Text();

    @Override public void map(final LongWritable key, final Text value, final Context context)
            throws IOException, InterruptedException {
      final String line = value.toString();
      final StringTokenizer tokenizer = new StringTokenizer(line, " \t\n\r.,:;-[]()_?!'\"");
      String curToken;
      while (tokenizer.hasMoreTokens()) {
        curToken = tokenizer.nextToken();
        if((curToken.charAt(0) >= 'a' && curToken.charAt(0) <= 'z') ||
                (curToken.charAt(0) >= 'A' && curToken.charAt(0) <= 'Z')) {
          word.set(curToken);
          context.write(word, ONE);
        }
      }
    }
  }

  static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (final IntWritable val : values){
        sum += val.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    final Configuration conf = this.getConf();
    final Job job = new Job(conf, "Word Count");
    job.setJarByClass(WordCount.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    //job.setCombinerClass(MyReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception{
    final int returnCode = ToolRunner.run(new Configuration(), new WordCount(), args);
    System.exit(returnCode);
  }
}