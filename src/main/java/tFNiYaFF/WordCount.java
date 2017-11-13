package tFNiYaFF;


import java.io.*;
import java.util.*;

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


public class WordCount extends Configured implements Tool {

  private static Map<Text, IntWritable> sortByValues(Map<Text,IntWritable> inputMap){
    Map<Text, IntWritable> sortedMap = new LinkedHashMap<>();
    ArrayList<Text> keys = new ArrayList<>();
    ArrayList<IntWritable> values = new ArrayList<>();
    for(Map.Entry<Text,IntWritable> pair: inputMap.entrySet()){
      keys.add(pair.getKey());
      values.add(pair.getValue());
    }
    for(int i=0; i<keys.size();i++){
      for(int j=0; j<keys.size()-1;j++){
        if(values.get(j).compareTo(values.get(j+1))==-1){
          IntWritable temp = values.get(j);
          values.set(j,values.get(j+1));
          values.set(j+1,temp);
          Text text = keys.get(j);
          keys.set(j,keys.get(j+1));
          keys.set(j+1,text);
        }
      }
    }
    for (int i=0; i<values.size();i++){
      sortedMap.put(keys.get(i),values.get(i));
    }
    return sortedMap;
  }

  public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private final transient Text word = new Text();

    @Override public void map(final LongWritable key, final Text value, final Context context)
      throws IOException, InterruptedException {
      final String line = value.toString();
      final StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        context.write(word, ONE);
      }
    }
  }


  public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Map<Text,IntWritable> countMap = new HashMap<>();

    @Override
    public void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
      throws IOException, InterruptedException {
      int sum = 0;
      for (final IntWritable val : values) {
        sum += val.get();
      }
      countMap.put(new Text(key),new IntWritable(sum));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      File file = new File("stop_words_en.txt");
      InputStreamReader isr = new InputStreamReader(new FileInputStream(file));
      BufferedReader br = new BufferedReader(isr);
      ArrayList<String> stopWords = new ArrayList<>();
      String line;
      while ((line = br.readLine()) != null) {
        stopWords.add(line);
      }
      Map<Text, IntWritable> sortedMap = sortByValues(countMap);
      int counter = 0;
      int stopWordsCounter = 0;
      int sum = 0;
      for (Text key: sortedMap.keySet()) {
        if(stopWords.contains(key.toString())){
          stopWordsCounter+=sortedMap.get(key).get();
        }
        sum+= sortedMap.get(key).get();
        if(++counter==7) {
          context.write(key, sortedMap.get(key));
        }
      }
      context.write(new Text( "stop% "), new IntWritable(stopWordsCounter/sum));
    }
  }


  @Override public int run(final String[] args) throws Exception {
    final Configuration conf = this.getConf();
    final Job job = Job.getInstance(conf, "Word Count");
    job.setJarByClass(WordCount.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(final String[] args) throws Exception {
    final int returnCode = ToolRunner.run(new Configuration(), new WordCount(), args);
    System.exit(returnCode);
  }
}
