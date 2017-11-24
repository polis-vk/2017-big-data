package kubrin;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StopWordsCount extends Configured implements Tool {
    private static Set<String> stopWords;

    private static void setStopWords(String stopWordsFile){
        stopWords = new HashSet<>();

        try {
            Files
                    .lines(Paths.get(stopWordsFile), StandardCharsets.UTF_8)
                    .forEach(stopWords::add);
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    enum MY_COUNTERS {
        WORDS_COUNT,
        STOP_WORDS_COUNT
    }


    static class MyMapper extends Mapper<Object, Text, Text, IntWritable>{
        private static final IntWritable ONE = new IntWritable(1);
        private final transient Text word = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final String line = value.toString();
            final StringTokenizer tokenizer = new StringTokenizer(line, " \n\t.,![]()-:;");
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, ONE);
            }
        }
    }

    static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            boolean isStopWord = stopWords.contains(key.toString());

            for (final IntWritable value : values) {
                context.getCounter(MY_COUNTERS.WORDS_COUNT).increment(value.get());

                if (isStopWord){
                    context.getCounter(MY_COUNTERS.STOP_WORDS_COUNT).increment(value.get());
                }
            }
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf = this.getConf();
        final Job job = new Job(conf, "StopWordsCount");

        setStopWords(args[2]);

        job.setJarByClass(StopWordsCount.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        long stopWordsCount = job.getCounters().findCounter(MY_COUNTERS.STOP_WORDS_COUNT).getValue();
        long wordsCount = job.getCounters().findCounter(MY_COUNTERS.WORDS_COUNT).getValue();
        double result = stopWordsCount / (double)wordsCount * 100;

        System.out.println(String.valueOf("Stop words percent in input files = " + result));

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        final int returnCode = ToolRunner.run(new Configuration(), new StopWordsCount(), args);
        System.exit(returnCode);
    }
}