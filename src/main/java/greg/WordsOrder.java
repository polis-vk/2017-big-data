package greg;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordsOrder {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    }
}
