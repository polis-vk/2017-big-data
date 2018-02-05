package mburmistrov.task1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private final transient Text word = new Text();

    @Override public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
      final String line = value.toString();
      final StringTokenizer tokenizer = new StringTokenizer(line, " \t\n\r.,:;-[]()_?!'\"");

      String cToken;

      while (tokenizer.hasMoreTokens()) {
        cToken = tokenizer.nextToken();
        if ((cToken.charAt(0) >= 'a' && cToken.charAt(0) <= 'z') || (cToken.charAt(0) >= 'A' && cToken.charAt(0) <= 'Z')) {
          word.set(cToken);
          context.write(word, ONE);
        }
      }
    }
  }