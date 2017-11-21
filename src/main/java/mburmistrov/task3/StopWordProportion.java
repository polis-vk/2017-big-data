package mburmistrov.task3;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

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

public class StopWordProportion extends Configured implements Tool {
    private static Set<String> stopWordSet;

    private static void setStopWords(String stopWordsFile){
        stopWordSet = new HashSet<>();
        try {
            Files.lines(Paths.get(stopWordsFile), StandardCharsets.UTF_8).forEach(stopWordSet::add);
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    static class StopWordProportionMapper extends Mapper<Object, Text, Text, IntWritable>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final String line = value.toString();

            int pos = line.indexOf(0x09);

            if (pos >= 0) {
                String inputString = line.substring(0, pos);
                int inputCount = Integer.valueOf(line.substring(pos + 1));

                context.write(new Text(inputString.toLowerCase()), new IntWritable(inputCount));
            }
        }
    }

    static class StopWordProportionReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            boolean isStopWord = stopWordSet.contains(key.toString());

            for (final IntWritable value : values) {

                context.getCounter(COUNTERS.WORD_COUNT).increment(value.get());

                if (isStopWord){
                    context.getCounter(COUNTERS.STOP_WORD_COUNT).increment(value.get());
                }
            }
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf = this.getConf();
        final Job job = new Job(conf, "StopWordProportion");

        setStopWords(args[2]);

        job.setJarByClass(StopWordProportion.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(StopWordProportionMapper.class);
        job.setReducerClass(StopWordProportionReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        long stopWordCount = job.getCounters().findCounter(COUNTERS.STOP_WORD_COUNT).getValue();
        long wordCount = job.getCounters().findCounter(COUNTERS.WORD_COUNT).getValue();

        double percentProportion = stopWordCount / (double) wordCount * 100;

        String outputPath = args[1] + "/output";
        File file = new File(outputPath);
        Files.write(file.toPath(), String.valueOf(percentProportion).getBytes());

        return success ? 0 : 1;
    }

    enum COUNTERS {
        WORD_COUNT,
        STOP_WORD_COUNT
    }

    public static void main(String[] args) throws Exception{
        final int returnCode = ToolRunner.run(new Configuration(), new StopWordProportion(), args);
        System.exit(returnCode);
    }
}
