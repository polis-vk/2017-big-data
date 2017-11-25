package olerom.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable val = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int totalSum = 0;
        for (IntWritable value : values) {
            totalSum += value.get();
        }
        val.set(totalSum);

        context.write(key, val);
    }
}