package nsuprotivniy;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextWithCountWriteble implements WritableComparable<TextWithCountWriteble>, Cloneable {
    private String text;
    private int count;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(count);
        dataOutput.writeUTF(text);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readInt();
        text = dataInput.readUTF();
    }

    public String getText() {
        return text;
    }

    public int getCount() {
        return count;
    }

    TextWithCountWriteble(){
        // should be
    }

    TextWithCountWriteble(String text, int count) {
        this.text = text;
        this.count = count;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null){
            return false;
        }
        if (other == this){
            return true;
        }
        if (!(other instanceof TextWithCountWriteble)){
            return false;
        }
        TextWithCountWriteble otherMyClass = (TextWithCountWriteble)other;
        if (otherMyClass.count != count){
            return false;
        }
        if (!otherMyClass.text.equals(text)){
            return false;
        }
        return true;
    }

    @Override
    protected TextWithCountWriteble clone() {
        return new TextWithCountWriteble(text, count);
    }

    @Override
    public int compareTo(TextWithCountWriteble o) {
        if (equals(o)){
            return 0;
        }
        int intCompare = Integer.compare(count, o.count);
        return (intCompare == 0) ? Integer.compare(this.hashCode(), o.hashCode()) : -intCompare;
    }
}
