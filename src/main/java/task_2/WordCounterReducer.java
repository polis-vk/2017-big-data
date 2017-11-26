package task_2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Created by iters on 10/22/17.
 */
public class WordCounterReducer
        extends Reducer<IntWritable, Text, Text, IntWritable> {

    private final IntWritable positiveInt = new IntWritable();
    private static Text text = new Text();
    private static final int OUT_POS = 7;
    private static int pos = 0;

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        positiveInt.set(Integer.parseInt(key.toString()) * (-1));
        for (Text value : values) {
            if (++pos == OUT_POS) {
                text.set("7-ое по популярности слово:\n"
                        + value.toString());
                context.write(text, positiveInt);
            }
        }
    }
}