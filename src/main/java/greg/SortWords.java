package greg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;

public class SortWords extends Configured implements Tool {

    private static int counter = 0;

    public static class MyMapper extends Mapper <LongWritable, Text, IntWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String[] data = value.toString().split("\t");
            context.write(new IntWritable(Integer.valueOf(data[1])), new Text(data[0]));
        }
    }

    public static class MyReducer extends Reducer <IntWritable, Text, Text, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            counter++;
            if(7 == counter) {
                System.out.println("\n\n\n\n\n\n\n\n\n\n\n" +
                        values.iterator().next().toString() +
                        "\t" + key.toString() + "\n\n\n\n\n\n\n\n\n\n");
            }
            super.reduce(key, values, context);
        }
    }

    public static class Comparetor extends WritableComparator {

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -Integer.compare(readInt(b1, s1), readInt(b2, s2));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        final Job job = new Job(new Configuration(), "sort words");
        job.setJarByClass(SortWords.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setSortComparatorClass(Comparetor.class);

        TextInputFormat.addInputPath(job, new Path(args[1]));
        TextOutputFormat.setOutputPath(job, new Path(args[1] + "_2"));

        job.setInputFormatClass(TextInputFormat.class);
        //job.setInputFormatClass(SequenceFileInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) throws Exception {

        if(args.length != 2) {
            throw new IllegalArgumentException("Usage: hadoop jar <name>.jar <Class> <folder_in> <folder_out>");
        }

        final int returnCode1 = ToolRunner.run(new Configuration(), new greg.WordCount(), args);
        final int returnCode2 = ToolRunner.run(new Configuration(), new greg.SortWords(), args);

        if(0 == returnCode1 && 0 == returnCode2)
            System.exit(0);
        System.exit(1);
    }
}
