package tFNiYaFF;

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

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NamesPercent extends Configured implements Tool {
    private static final Pattern namePattern = Pattern.compile("^[A-Z][a-z0-9]*$");

    static class MyMapper extends Mapper<Object, Text, Text, TextWithCountWriteble> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final String line = value.toString();

            int pos = line.indexOf(0x09);

            String inputString = line.substring(0, pos);
            int inputCount = Integer.valueOf(line.substring(pos+1));

            context.write(new Text(inputString.toLowerCase()), new TextWithCountWriteble(inputString, inputCount));
        }
    }

    static class MyReducer extends Reducer<Text, TextWithCountWriteble, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<TextWithCountWriteble> values, Context context) throws IOException, InterruptedException {
            int sumAllForms = 0;
            int rightFormCount = 0;
            String rightFormText = null;

            for (final TextWithCountWriteble value : values){
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
        job.setJarByClass(NamesPercent.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextWithCountWriteble.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        final int returnCode = ToolRunner.run(new Configuration(), new NamesPercent(), args);
        System.exit(returnCode);

    }
}
