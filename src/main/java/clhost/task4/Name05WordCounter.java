package clhost.task4;

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
import java.util.StringTokenizer;

public class Name05WordCounter extends Configured implements Tool {

    public static class Name05WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final transient IntWritable count = new IntWritable();
        private final transient Text word = new Text();

        @Override // на вход файл из Word Count
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            System.out.println("#map");
            final String line = value.toString();
            final StringTokenizer tokenizer = new StringTokenizer(line);
            int count_all = 0;

            while (tokenizer.hasMoreTokens()) {
                count_all++; // количество записей
            }

            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                word.set(token.split(" ")[0]);
                count.set(Integer.parseInt(token.split(" ")[1]) * (-1));

                if (((double) count.get()) / count_all <= 0.5) {
                    context.write(word, new IntWritable(count.get()));
                }

            }
        }
    }


    public static class Name05WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override // ничего не делает
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            System.out.println("#reduce");
            for (final IntWritable val : values) {
                context.write(key, val);
            }
        }
    }


    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = this.getConf();
        final Job job = Job.getInstance(conf, "Name05 Word Count");
        job.setJarByClass(Name05WordCounter.class);

        job.setMapperClass(Name05WordCounter.Name05WordMapper.class);
        job.setReducerClass(Name05WordCounter.Name05WordReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/home/clhost/proj/2017-big-data/wordcountoutput/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("/home/clhost/proj/2017-big-data/name05wordcountoutput"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) throws Exception {
        final int returnCode = ToolRunner.run(new Configuration(), new Name05WordCounter(), args);
        System.exit(returnCode);
    }
}
