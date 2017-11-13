package clhost.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

// подаем на вход результат работы джобы WordCount
public class FrequencyWordCounter extends Configured implements Tool {

    public static class FrequencyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final Text word = new Text();
        private final IntWritable count = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");

            word.set(tokens[0]);
            count.set((-1) * Integer.parseInt(tokens[1]));
            context.write(count, word);
        }
    }


    public static class FrequencyReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        private final IntWritable count = new IntWritable();
        private int counter = 0;

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("#reduce");
            count.set(Integer.parseInt(key.toString()) * (-1));
            for (Text value : values) {
                context.write(value, count);
            }
            counter++;


            /*if (counter == 7) {
                for (Text value : values) {
                    context.write(value, count);
                }
            }
            counter++;*/
        }
    }


    @Override
    public int run(final String[] args) throws Exception {
        Job job = new Job(getConf(), "Frequency Word Count");
        job.setJarByClass(getClass());

        TextInputFormat.addInputPath(job,
                new Path("/home/clhost/proj/2017-big-data/wordcountoutput/part-r-00000"));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(FrequencyMapper.class);
        job.setReducerClass(FrequencyReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        TextOutputFormat.setOutputPath(job,
                new Path("/home/clhost/proj/2017-big-data/frequencycountoutput"));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) throws Exception {
        final int returnCode = ToolRunner.run(new Configuration(), new FrequencyWordCounter(), args);
        System.exit(returnCode);
    }
}
