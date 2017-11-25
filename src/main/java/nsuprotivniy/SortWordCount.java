package nsuprotivniy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class SortWordCount extends WordCount {

    static class InverseMapper extends Mapper<Object, Text, IntWritable, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws InterruptedException, IOException {
            String[] pair = value.toString().split("\t");
            context.write(new IntWritable(Integer.valueOf(pair[1])), new Text(pair[0]));
        }
    }

    public static class MyDescFreqComparator extends WritableComparator {
        protected MyDescFreqComparator() {
            super(IntWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -Integer.compare(readInt(b1, s1), readInt(b2, s2));
        }
    }

    public static class InverseReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "sort word count");
        job.setJarByClass(SortWordCount.class);
        job.setMapperClass(InverseMapper.class);
        job.setReducerClass(InverseReducer.class);
        job.setSortComparatorClass(MyDescFreqComparator.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        if (job.waitForCompletion(true))
            return 0;
        else
            return 1;
    }

    public static void main(String[] args) throws Exception {
        final int exitCode = ToolRunner.run(new SortWordCount(), args);
        System.exit(exitCode);
    }
}