package tFNiYaFF;

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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

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


    static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final String line = value.toString();

            int pos = line.indexOf(0x09);

            String inputString = line.substring(0, pos);
            int inputCount = Integer.valueOf(line.substring(pos+1));

            context.write(new Text(inputString.toLowerCase()), new IntWritable(inputCount));
        }
    }

    static class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int valuesCount = 0;

            for (final IntWritable value : values) {
                valuesCount += value.get();
            }

            context.write(key, new IntWritable(valuesCount));
        }
    }

    static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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

        // третьим аргументом приходит файл стоп-слов
        setStopWords(args[2]);

        job.setJarByClass(StopWordsCount.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setCombinerClass(MyCombiner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // читаем из выходной папки wordcount
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // в выходную папку положится пустой файл (в context ничего не пишем)
        // и в этой же папке создастся файл "output", в котором будет результат работы программы
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        long stopWordsCount = job.getCounters().findCounter(MY_COUNTERS.STOP_WORDS_COUNT).getValue();
        long wordsCount = job.getCounters().findCounter(MY_COUNTERS.WORDS_COUNT).getValue();
        double percent = stopWordsCount / (double)wordsCount * 100;

        File file = new File(args[1] + "/output");
        Files.write(file.toPath(), String.valueOf(percent).getBytes());

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        final int returnCode = ToolRunner.run(new Configuration(), new StopWordsCount(), args);
        System.exit(returnCode);
    }
}
