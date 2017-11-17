package task_4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by iters on 10/22/17.
 */
public class WordCounterMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer st = new StringTokenizer(value.toString());
        String str, temp;

        while (st.hasMoreTokens()) {
            str = st.nextToken();
            temp = str;
            str = str.toLowerCase();
            str = str.substring(0, 1).toUpperCase()
                    + str.substring(1, str.length());

            word.set(str.toLowerCase());

            boolean isDigit = Character.isDigit(str.charAt(0));
            if (temp.equals(str) && !isDigit) {
                context.write(word, new IntWritable(1));
            } else {
                context.write(word, new IntWritable(-1));
            }
        }
    }
}