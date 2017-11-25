package olerom.namecount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Date: 30.10.17
 *
 * @author olerom
 */
public class NameCountMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable ONE = new IntWritable(1);
    private final Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        while (tokenizer.hasMoreTokens()) {
            final String token = tokenizer.nextToken();
            if (!Character.isDigit(token.charAt(0))) {
                if (isName(token)) {
                    word.set(token);
                    context.write(word, ONE);
                } else {
                    context.getCounter("groupName", token.toLowerCase()).increment(1);
                }
            }
        }
    }

    private boolean isName(final String token) {
        if (token == null || token.length() == 0) {
            return false;
        }

        final char firstLetter = token.charAt(0);
        final String otherLetters = token.substring(1, token.length());
        final Pattern pattern = Pattern.compile("[a-z0-9]+");
        final Matcher matcher = pattern.matcher(otherLetters);

        return Character.isUpperCase(firstLetter) && matcher.matches();
    }
}