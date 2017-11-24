package kubrin;
// based on implementation of a-filippo (https://github.com/a-filippo/2017-big-data)

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

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

public class GetRow extends Configured implements Tool {
    private static int valueNumber;
    private static IntWritable ONE = new IntWritable(1);

    static class MyMapper extends Mapper<Object, Text, IntWritable, TextWithCountWritable>{
        private int count;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            count = 0;
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final String line = value.toString();

            int pos = line.indexOf(0x09);

            int inputCount = Integer.valueOf(line.substring(0, pos));
            String inputString = line.substring(pos+1);

            if (count <= valueNumber){
                context.write(ONE, new TextWithCountWritable(inputString, inputCount));
                count++;
            }
        }
    }

    static class MyReducer extends Reducer<IntWritable, TextWithCountWritable, Text, IntWritable>{
        private SortedSet<TextWithCountWritable> setOfTextWithCount;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            setOfTextWithCount = new TreeSet<>();
        }

        @Override
        protected void reduce(IntWritable key, Iterable<TextWithCountWritable> values, Context context) throws IOException, InterruptedException {
            values.forEach((textWithCountWriteble -> {
                setOfTextWithCount.add(textWithCountWriteble.clone());
                if (setOfTextWithCount.size() > valueNumber + 1){
                    setOfTextWithCount.remove(setOfTextWithCount.last());
                }
            }));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);

            int i = 0;
            for (TextWithCountWritable textWithCountWriteble : setOfTextWithCount){
                if (i == valueNumber){
                    context.write(new Text(textWithCountWriteble.getText()), new IntWritable(textWithCountWriteble.getCount()));
                    break;
                }
                i++;
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf = this.getConf();
        final Job job = new Job(conf, "GetRow");

        valueNumber = Integer.valueOf(args[2]);

        job.setJarByClass(GetRow.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(TextWithCountWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        final int returnCode = ToolRunner.run(new Configuration(), new GetRow(), args);
        System.exit(returnCode);

    }
}