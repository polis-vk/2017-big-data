package greg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.StringTokenizer;

public class StopWords extends Configured implements Tool {

    private static HashSet<String> stopWords = new HashSet<String>();

    public static void readStopWords(String stopWordsFilePath) {
        try {
            Scanner sc = new Scanner(new File(stopWordsFilePath));
            while(sc.hasNext()) {
                stopWords.add(sc.nextLine());
            }
        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        }
    }

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();
                context.getCounter(MATCH_COUNTER.ALL).increment(1);
                if (stopWords.contains(word)) {
                    System.out.println("Contains: " + word);
                    context.getCounter(MATCH_COUNTER.STOP_WORD).increment(1);
                }
            }
        }
    }

    public enum MATCH_COUNTER {
        STOP_WORD,
        ALL
    }

    @Override
    public int run(String[] args) throws Exception {

        final Job job = new Job(new Configuration(), "stop words");
        job.setJarByClass(StopWords.class);

        job.setMapperClass(MyMapper.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setMapOutputKeyClass(IntWritable.class);
//        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        int exitCode = job.waitForCompletion(true) ? 0 : 1;

        Counters counter = job.getCounters();

        System.out.println("%: "
                + ((double) counter.findCounter(MATCH_COUNTER.STOP_WORD).getValue()
                / counter.findCounter(MATCH_COUNTER.ALL).getValue()));

        return exitCode;
    }

    public static void main(String[] args) throws Exception {

        if(args.length != 3) {
            throw new IllegalArgumentException("Usage: hadoop jar <name>.jar <Class> <file_in> <folder_out> <stop_words_file>");
        }

        readStopWords(args[2]);

        final int returnCode = ToolRunner.run(new Configuration(), new greg.StopWords(), args);

        System.exit(returnCode);
    }
}
