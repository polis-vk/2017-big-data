package kubrin;

import java.io.IOException;

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

public class SortWords extends Configured implements Tool {
    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final transient Text word = new Text();

        @Override public void map(final LongWritable key, final Text value, final Context context)
                throws IOException, InterruptedException {
            final String line = value.toString();
            word.set(line.split("\t")[0]);
            int val = -1*Integer.parseInt(line.split("\t")[1]);
            context.write(new IntWritable(val), word);
        }
    }

    static class MyReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (final Text value : values) {
                context.write(key, value);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf = this.getConf();
        final Job job = new Job(conf, "Words Order");
        job.setJarByClass(SortWords.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MyMapper.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        final int returnCode = ToolRunner.run(new Configuration(), new SortWords(), args);
        System.exit(returnCode);
    }
}
