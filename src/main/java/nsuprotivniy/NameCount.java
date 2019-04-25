package nsuprotivniy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class NameCount extends Configured implements Tool {

    static class MyMapper extends Mapper<Object, Text, Text, TextWithCountWriteble>{

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] pair = value.toString().split("\t");
            context.write(new Text(pair[0].toLowerCase()), new TextWithCountWriteble(pair[0], Integer.valueOf(pair[1])));

        }
    }

    static class MyReducer extends Reducer<Text, TextWithCountWriteble, Text, IntWritable>{

        private Pattern namePattern =  Pattern.compile("[A-Z][a-z0-9]*");


        @Override
        protected void reduce(Text key, Iterable<TextWithCountWriteble> values, Context context) throws IOException, InterruptedException {
            int sumAllForms = 0;
            int rightFormCount = 0;
            String rightFormText = null;

            for (final TextWithCountWriteble value : values){
                sumAllForms += value.getCount();
                Matcher matcher = namePattern.matcher(value.getText());
                if (matcher.matches()) {
                    rightFormText = value.getText();
                    rightFormCount += value.getCount();
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
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length != 2) {
            System.err.println("Usage: NameCount <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "name count");
        job.setJarByClass(NameCount.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextWithCountWriteble.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

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

