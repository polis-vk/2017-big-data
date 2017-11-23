package mburmistrov.task2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TextWCount implements WritableComparable<TextWCount>, Cloneable {

  private int count;
  private String text;


  @Override
  public void readFields(DataInput dInput) throws IOException {
    count = dInput.readInt();

    text = dInput.readUTF();
  }

  @Override
  public void write(DataOutput dOutput) throws IOException {
    dOutput.writeInt(count);

    dOutput.writeUTF(text);
  }


  public TextWCount(String text, int count) {
    this.text = text;

    this.count = count;
  }

  @Override
  public boolean equals(Object other) {

    if (other == null) {
      return false;
    }

    if (other == this) {
      return true;
    }

    if (!(other instanceof TextWCount)) {
      return false;
    }

    TextWCount otherMyClass = (TextWCount) other;

    if (!otherMyClass.text.equals(text)) {
      return false;
    }

    if (otherMyClass.count != count) {
      return false;
    }

    return true;
  }

  @Override
  protected TextWCount clone() {
    return new TextWCount(text, count);
  }

  TextWCount() {
  }

  public String getText() {
    return text;
  }

  public int getCount() {
    return count;
  }

  @Override
  public int compareTo(TextWCount o) {
    if (equals(o)) {
      return 0;
    }
    int intCompare = Integer.compare(count, o.count);
    if (intCompare == 0) {
      return Integer.compare(this.hashCode(), o.hashCode());
    } else {
      return -intCompare;
    }
  }
}