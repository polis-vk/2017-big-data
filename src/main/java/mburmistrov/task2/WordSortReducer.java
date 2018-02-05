package mburmistrov.task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordSortReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
  @Override
  protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    for (final Text value : values) {
      context.write(key, value);
    }
  }
}
