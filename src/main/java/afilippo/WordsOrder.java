package afilippo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordsOrder extends Configured implements Tool {

    static class MyMapper extends Mapper<Object, Text, IntWritable, Text>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final String line = value.toString();

            int pos = line.indexOf(0x09);

            context.write(new IntWritable(Integer.valueOf(line.substring(pos+1))), new Text(line.substring(0, pos)));
        }
    }

    static class MyReducer extends Reducer<IntWritable, Text, IntWritable, Text>{

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (final Text value : values){
                context.write(key, value);
            }
        }
    }

    static class MyDescFreqComparator extends WritableComparator{
        protected MyDescFreqComparator() {
            super(IntWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -Integer.compare(readInt(b1, s1), readInt(b2, s2));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf = this.getConf();
        final Job job = new Job(conf, "Words Order");
        job.setJarByClass(WordsOrder.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setSortComparatorClass(MyDescFreqComparator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        final int returnCode = ToolRunner.run(new Configuration(), new WordsOrder(), args);
        System.exit(returnCode);
    }
}
