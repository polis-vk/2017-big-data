package mburmistrov.task4;

import mburmistrov.task2.TextWCount;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NameWordProportionMapper extends Mapper<Object, Text, Text, TextWCount> {

  @Override
  protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    final String line = value.toString();

    int pos = line.indexOf(0x09);

    if (pos >= 0) {

      String inputString = line.substring(0, pos);
      int inputCount = Integer.valueOf(line.substring(pos + 1));

      context.write(new Text(inputString.toLowerCase()), new TextWCount(inputString, inputCount));
    }
  }
}