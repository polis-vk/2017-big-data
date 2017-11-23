package mburmistrov.task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  public void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
          throws IOException, InterruptedException {
    int sum = 0;
    for (final IntWritable val : values) {
      sum += val.get();
    }
    context.write(key, new IntWritable(sum));
  }
}