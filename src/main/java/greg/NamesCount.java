package greg;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NamesCount extends Configured implements Tool {

    private static final Pattern namePattern = Pattern.compile("^[A-Z][a-z0-9]*$");

    public static class MyMapper extends Mapper<LongWritable, Text, Text, TextWithCountWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String[] data = value.toString().split("\t");

            String inputString = data[1];
            int inputCount = Integer.valueOf(data[0]);

            context.write(new Text(inputString.toLowerCase()), new TextWithCountWritable(inputString, inputCount));
        }
    }

    static class MyReducer extends Reducer<Text, TextWithCountWritable, Text, IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<TextWithCountWritable> values, Context context) throws IOException, InterruptedException {
            int allCount = 0;
            int asNameCount= 0;
            String asNameText = null;

            for (final TextWithCountWritable value : values){
                allCount += value.getCount();

                if (asNameText == null){
                    Matcher matcher = namePattern.matcher(value.getText());
                    if (matcher.matches()){
                        asNameText = value.getText();
                        asNameCount = value.getCount();
                    }
                }
            }

            if (asNameText == null){
                return;
            }

            if (asNameCount / (double)allCount >= 0.995){
                context.write(new Text(asNameText), new IntWritable(asNameCount));
            }
        }
    }


    @Override
    public int run(String[] args) throws Exception {

        final Job job = new Job(new Configuration(), "count names");
        job.setJarByClass(NamesCount.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextWithCountWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if(args.length != 2) {
            throw new IllegalArgumentException("Usage: hadoop jar <name>.jar <Class> <folder_in> <folder_out>");
        }

        final int returnCode = ToolRunner.run(new Configuration(), new NamesCount(), args);

        System.exit(returnCode);
    }
}
