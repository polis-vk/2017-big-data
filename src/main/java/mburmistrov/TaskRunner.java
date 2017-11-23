package mburmistrov;

import mburmistrov.task1.WordCount;
import mburmistrov.task2.WordCertainPosition;
import mburmistrov.task2.WordSort;
import mburmistrov.task3.StopWordProportion;
import mburmistrov.task4.NameWordProportion;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;

public class TaskRunner {
  public static void main(String[] args) throws Exception{

    // task 1
    ToolRunner.run(
      new Configuration(),
      new WordCount(),
      new String[]{"input", "output/1_wordCount"}
    );

    // task 2
    ToolRunner.run(
      new Configuration(),
      new WordSort(),
      new String[]{"output/1_wordCount", "output/2_wordSort"}
    );

    ToolRunner.run(
      new Configuration(),
      new WordCertainPosition(),
      new String[]{"output/2_wordSort", "output/2_seventhWord", "6"}
    );

    // task 3
    ToolRunner.run(
      new Configuration(),
      new StopWordProportion(),
      new String[]{"output/1_wordCount", "output/3_stopWordProportion", "resources/stop_words_en.txt"}
    );

    // task 4
    ToolRunner.run(
      new Configuration(),
      new NameWordProportion(),
      new String[]{"output/1_wordCount", "output/4_nameWordProportion"}
    );

    ToolRunner.run(
      new Configuration(),
      new WordSort(),
      new String[]{"output/4_nameWordProportion", "output/4_nameWordSort"}
    );

    ToolRunner.run(
      new Configuration(),
      new WordCertainPosition(),
      new String[]{"output/4_nameWordSort", "output/4_nameFifth", "4"}
    );

  }

}
