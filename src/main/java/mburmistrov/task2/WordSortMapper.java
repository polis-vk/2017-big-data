package mburmistrov.task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordSortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

  @Override
  protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
    final String line = value.toString();
    final String[] splitLine = line.split("\t");

    context.write(new IntWritable(Integer.valueOf(splitLine[1])), new Text(splitLine[0]));
  }
}