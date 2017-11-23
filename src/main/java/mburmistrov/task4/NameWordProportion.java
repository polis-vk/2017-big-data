package mburmistrov.task4;

import mburmistrov.task2.TextWCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NameWordProportion extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    final Configuration conf = this.getConf();
    final Job job = new Job(conf, "NameWordProportion");
    job.setJarByClass(NameWordProportion.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(NameWordProportionMapper.class);
    job.setReducerClass(NameWordProportionReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(TextWCount.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    final int returnCode = ToolRunner.run(new Configuration(), new NameWordProportion(), args);
    System.exit(returnCode);

  }
}
