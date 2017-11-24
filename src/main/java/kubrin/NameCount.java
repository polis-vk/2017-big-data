package kubrin;
// based on implementation of a-filippo (https://github.com/a-filippo/2017-big-data)

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class NameCount extends Configured implements Tool {
    private static final Pattern namePattern = Pattern.compile("^[A-Z][a-z0-9]*$");

    static class MyMapper extends Mapper<Object, Text, Text, TextWithCountWritable>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final String line = value.toString();

            int pos = line.indexOf('\t');

            String inputString = line.substring(0, pos);
            int inputCount = Integer.valueOf(line.substring(pos+1));

            context.write(new Text(inputString.toLowerCase()), new TextWithCountWritable(inputString, inputCount));
        }
    }

    static class MyReducer extends Reducer<Text, TextWithCountWritable, Text, IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<TextWithCountWritable> values, Context context) throws IOException, InterruptedException {
            int sumAllForms = 0;
            int rightFormCount = 0;
            String rightFormText = null;

            for (final TextWithCountWritable value : values){
                sumAllForms += value.getCount();

                if (rightFormText == null){
                    Matcher matcher = namePattern.matcher(value.getText());
                    if (matcher.matches()){
                        rightFormText = value.getText();
                        rightFormCount = value.getCount();
                    }
                }
            }

            if (rightFormText == null){
                return;
            }

            if (rightFormCount / (double)sumAllForms >= 0.995){
                context.write(new Text(rightFormText), new IntWritable(rightFormCount));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf = this.getConf();
        final Job job = new Job(conf, "NamesPercent");
        job.setJarByClass(NameCount.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextWithCountWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        final int returnCode = ToolRunner.run(new Configuration(), new NameCount(), args);
        System.exit(returnCode);

    }
}