package nsuprotivniy;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeSet;


public class StopWordsCount extends Configured implements Tool {

    public enum MATCH_COUNTER {
        STOP_WORD,
        TOTAL
    }

    public static class StopWordCountMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final HashSet<String> stopWords = new HashSet<>();


        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            Configuration conf = context.getConfiguration();
            URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
            for (URI patternsURI : patternsURIs) {
                Path patternsPath = new Path(patternsURI.getPath());
                String patternsFileName = patternsPath.getName().toString();
                loadStopWords(patternsFileName);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());

            while (tokenizer.hasMoreTokens()) {
                String trim = tokenizer.nextToken().trim();
                context.getCounter(MATCH_COUNTER.TOTAL).increment(1);
                if (stopWords.contains(trim)) {
                    context.getCounter(MATCH_COUNTER.STOP_WORD).increment(1);
                }
            }
        }


        private void loadStopWords(String path) throws IOException {
            BufferedReader file = new BufferedReader(new FileReader(path));
            String stop_word;
            while ((stop_word = file.readLine()) != null) {
                stopWords.add(stop_word);
            }
        }
    }




    public int run(final String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length != 4) {
            System.err.println("Usage: StopWordsCount <in> <out> [-stopwords StopWordsFile]");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "stop words count");
        job.setJarByClass(StopWordsCount.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.addCacheFile(new Path(args[3]).toUri());

        job.setMapperClass(StopWordCountMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        int exit = job.waitForCompletion(true) ? 0 : 1;
        Counters counter = job.getCounters();


        long stop_words = counter.findCounter(MATCH_COUNTER.STOP_WORD).getValue();
        long total = counter.findCounter(MATCH_COUNTER.TOTAL).getValue();
        long percentage = stop_words / total;

        System.out.println("Stop words: " + stop_words);
        System.out.println("Total: " + total);
        System.out.println("Percentage: " + percentage);

        return exit;
    }

    public static void main(String[] args) throws Exception {
        final int exitCode = ToolRunner.run(new StopWordsCount(), args);
        System.exit(exitCode);
    }
}

