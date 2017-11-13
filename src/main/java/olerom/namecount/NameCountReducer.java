package olerom.namecount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Date: 13.11.17
 *
 * @author olerom
 */
public class NameCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        System.out.println();

        int totalSum = 0;
        for (IntWritable value : values) {
            totalSum += value.get();
        }

        System.out.println("1: " + totalSum);
        totalSum += Math.round(context.getCounter("groupName", key.toString().toLowerCase()).getValue()
                / (totalSum * 0.005));
        System.out.println(context.getCounter("groupName", key.toString().toLowerCase()).getValue());
        System.out.println("2: " + totalSum);

        context.write(key, new IntWritable(totalSum));
    }
}