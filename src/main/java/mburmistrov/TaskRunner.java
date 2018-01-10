package mburmistrov;

import mburmistrov.task1.WordCount;
import mburmistrov.task2.WordCertainPosition;
import mburmistrov.task2.WordSort;
import mburmistrov.task3.StopWordProportion;
import mburmistrov.task4.NameWordProportion;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;

import java.nio.file.Files;
import java.nio.file.Paths;

public class TaskRunner {
  private static double getTaskTime(Tool tool, String... parameters) throws Exception{
    long timeFrom = System.currentTimeMillis();

    ToolRunner.run(new Configuration(), tool, parameters);

    long timeTo = System.currentTimeMillis();
    return (timeTo - timeFrom) / (double) 1000;
  }

  public static void main(String[] args) throws Exception {

    // task 1
    TaskManager t1 = new TaskManager("task 1").performTask("word count", getTaskTime(new WordCount(),"input", "output/1_wordCount"));

    // task 2
    TaskManager t2 = new TaskManager("task 2")
            .performTask("word sort", getTaskTime(new WordSort(),"output/1_wordCount", "output/2_wordSort"))
            .performTask("output seventh word", getTaskTime(new WordCertainPosition(),"output/2_wordSort", "output/2_seventhWord", "6"));

    // task 3
    TaskManager t3 = new TaskManager("task 3").performTask("stop word proportion", getTaskTime(new StopWordProportion(),"output/1_wordCount", "output/3_stopWordProportion", "resources/stop_words_en.txt"));

    // task 4
    TaskManager t4 = new TaskManager("task 4")
            .performTask("name word proportion", getTaskTime(new NameWordProportion(),"output/1_wordCount", "output/4_nameWordProportion"))
            .performTask("name word sort", getTaskTime(new WordSort(),"output/4_nameWordProportion", "output/4_nameWordSort"))
            .performTask("name fifth", getTaskTime(new WordCertainPosition(),"output/4_nameWordSort", "output/4_nameFifth", "4"));

    Files.write(Paths.get("output/timeReport"), String.join("\r\n\r\n", t1.getString(), t2.getString(), t3.getString(), t4.getString()).getBytes());
  }

}
