package clhost.task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StopWordCounter extends Configured implements Tool {

    public static class StopWordMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private HashSet<String> set = initSet();

        @Override
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            System.out.println("#map");
            final String line = value.toString();
            final StringTokenizer tokenizer = new StringTokenizer(line);

            int count_valid = 0;
            int count_all = 0;
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();

                if (set.contains(word.trim())) {
                    count_valid++;
                }
                count_all++;
            }
            context.write(new IntWritable(count_valid), new IntWritable(count_all));
        }

        private HashSet<String> initSet() {
            String path = "/home/clhost/proj/2017-big-data/stop_words_en.txt";
            Set<String> set = new HashSet<>();

            try (Stream<String> stream = Files.lines(Paths.get(path))) {
                set = stream
                        .collect(Collectors.toSet());
            } catch (IOException e) {
                e.printStackTrace();
            }
            return (HashSet<String>) set;
        }
    }


    public static class StopWordReducer extends Reducer<IntWritable, IntWritable, IntWritable, DoubleWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("#reduce");
            // приходит 2 значения
            int count_valid = key.get();
            int count_all = 0;

            for (IntWritable ci : values) {
                count_all = ci.get(); // вернет одно значение
            }

            // return (1, %)
            context.write(new IntWritable(1), new DoubleWritable(((double) count_valid) / count_all));
        }
    }


    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = this.getConf();
        final Job job = Job.getInstance(conf, "Stop Word Count");
        job.setJarByClass(StopWordCounter.class);

        job.setMapperClass(StopWordCounter.StopWordMapper.class);
        job.setReducerClass(StopWordCounter.StopWordReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("/home/clhost/proj/2017-big-data/stopwordcountoutput"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) throws Exception {
        final int returnCode = ToolRunner.run(new Configuration(), new StopWordCounter(), args);
        System.exit(returnCode);
    }
}
