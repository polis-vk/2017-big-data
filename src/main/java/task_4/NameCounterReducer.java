package task_4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by iters on 11/13/17.
 */
public class NameCounterReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable digit = new IntWritable();
    private final Text tName = new Text();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Double sumPositive = 0d;
        Double sumNegative = 0d;

        for (IntWritable num : values) {
            int res = num.get();

            if (res == 1) {
                sumPositive++;
            } else {
                sumNegative++;
            }
        }

        if (Double.compare(sumNegative / (sumPositive + sumNegative) * 100
                , 0.5) < 0) {
            digit.set(sumPositive.intValue());
            tName.set(key.toString().substring(0, 1).toUpperCase()
                    + key.toString().substring(1, key.toString().length()));

            context.write(tName, digit);
        }
    }
}
