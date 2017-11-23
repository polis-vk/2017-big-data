package mburmistrov.task4;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mburmistrov.task2.TextWCount;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NameWordProportionReducer extends Reducer<Text, TextWCount, Text, IntWritable> {
  private static final Pattern nameWordPattern = Pattern.compile("^[A-Z][a-z0-9]*$");

  @Override
  protected void reduce(Text key, Iterable<TextWCount> values, Context context) throws IOException, InterruptedException {
    int allCount = 0;
    int correctCount = 0;
    String correctText = null;
    double correctPercent = 0.995;

    for (final TextWCount value : values) {

      allCount += value.getCount();

      if (correctText == null) {
        Matcher matcher = nameWordPattern.matcher(value.getText());

        if (matcher.matches()) {
          correctText = value.getText();

          correctCount = value.getCount();
        }
      }
    }

    if (correctText == null) {
      return;
    }

    if (correctCount / (double) allCount >= correctPercent) {
      context.write(new Text(correctText), new IntWritable(correctCount));
    }
  }
}