package task_3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Created by iters on 10/22/17.
 */
public class WordCounterMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static Set<String> stopWords = new HashSet<>();

    static {
        InputStream is = WordCounterMapper.class.getClassLoader().getResourceAsStream("StopWords");
        try (BufferedReader bf = new BufferedReader(new InputStreamReader(is))) {
            String line;

            while ((line = bf.readLine()) != null) {
                stopWords.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        StringTokenizer st = new StringTokenizer(value.toString());
        while(st.hasMoreTokens()) {
            if (stopWords.contains(st.nextToken())) {
                context.getCounter(
                        StopWordProportion.stopWordProportion.STOP_WORD).increment(1);
            } else {
                context.getCounter(
                        StopWordProportion.stopWordProportion.COMMON_WORD).increment(1);
            }
        }
    }
}