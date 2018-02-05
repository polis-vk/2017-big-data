package mburmistrov;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public  class TaskManager {
  private List<String> stringList;

  TaskManager(String taskName) {
    stringList = new ArrayList<>();

    stringList.add(taskName);
  }

  TaskManager performTask(String description, double time){
    stringList.add(String.format("%.3f", time) + "s" + " - " + description);

    return this;
  }

  public String getString() throws IOException {
    return String.join("\r\n", stringList);
  }
}