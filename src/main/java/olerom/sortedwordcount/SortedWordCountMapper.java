package olerom.sortedwordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

/**
 * Date: 12.11.17
 *
 * @author olerom
 */
public class SortedWordCountMapper extends MapReduceBase implements Mapper<Object, Text, IntWritable, Text> {

    public void map(Object key, Text value, OutputCollector<IntWritable, Text> collector, Reporter arg3) throws IOException {
        String line = value.toString();
        StringTokenizer stringTokenizer = new StringTokenizer(line);
        {
            int number = -1;
            String word = "empty";

            if (stringTokenizer.hasMoreTokens()) {
                String str0 = stringTokenizer.nextToken();
                word = str0.trim();
            }

            if (stringTokenizer.hasMoreElements()) {
                String str1 = stringTokenizer.nextToken();
                number = (-1) * Integer.parseInt(str1.trim());
            }
            if (number != 1)
                collector.collect(new IntWritable(number), new Text(word));
        }

    }
}