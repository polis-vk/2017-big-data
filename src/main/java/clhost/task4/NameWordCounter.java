package clhost.task4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import java.util.StringTokenizer;

public class NameWordCounter extends Configured implements Tool {

    public static class NameWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private final transient Text word = new Text();

        @Override
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            System.out.println("#map");
            final String line = value.toString();
            final StringTokenizer tokenizer = new StringTokenizer(line);

            while (tokenizer.hasMoreTokens()) {
                String w = tokenizer.nextToken();
                if (w.matches("[A-Z].+")) {
                    word.set(w);
                    context.write(word, ONE);
                }
            }
        }
    }


    public static class NameWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            System.out.println("#reduce");
            int sum = 0;
            for (final IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }


    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = this.getConf();
        final Job job = Job.getInstance(conf, "Name Word Count");
        job.setJarByClass(NameWordCounter.class);

        job.setMapperClass(NameWordCounter.NameWordMapper.class);
        job.setReducerClass(NameWordCounter.NameWordReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("/home/clhost/proj/2017-big-data/namewordcountoutput"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) throws Exception {
        final int returnCode = ToolRunner.run(new Configuration(), new NameWordCounter(), args);
        System.exit(returnCode);
    }
}
