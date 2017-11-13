package olerom.sortedwordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Date: 30.10.17
 *
 * @author olerom
 */
public class SortedWordCountReducer extends MapReduceBase implements Reducer<IntWritable, Text, Text, IntWritable> {

    public void reduce(IntWritable key,
                       Iterator<Text> values,
                       OutputCollector<Text, IntWritable> collector,
                       Reporter reporter) throws IOException {
        while ((values.hasNext())) {
            collector.collect(values.next(), new IntWritable(-1 * key.get()));
        }
    }

}