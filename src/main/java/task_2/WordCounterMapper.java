package task_2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by iters on 10/22/17.
 */
public class WordCounterMapper
        extends Mapper<LongWritable, Text, IntWritable, Text> {

    private final Text word = new Text();
    private final IntWritable num = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");

        word.set(parts[0]);
        num.set(-1 * Integer.parseInt(parts[1]));
        context.write(num, word);
    }
}