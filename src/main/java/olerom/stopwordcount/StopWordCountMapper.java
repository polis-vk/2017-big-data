package olerom.stopwordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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
public class StopWordCountMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final HashSet<String> stopWords = new HashSet<>();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        shittySetup(context.getConfiguration().get("stopWords"));

        while (tokenizer.hasMoreTokens()) {
            String trim = tokenizer.nextToken().trim();
            context.getCounter(MATCH_COUNTER.TOTAL).increment(1);
            if (stopWords.contains(trim)) {
                System.out.println("Contains: " + trim);
                context.getCounter(MATCH_COUNTER.STOP_WORD).increment(1);
            }
        }
    }

    public enum MATCH_COUNTER {
        STOP_WORD,
        TOTAL
    }


    private void shittySetup(String path) {
        BufferedReader br = null;
        FileReader fr = null;
        try {
            fr = new FileReader(path);
            br = new BufferedReader(fr);
            String sCurrentLine;
            while ((sCurrentLine = br.readLine()) != null) {
                stopWords.add(sCurrentLine);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}