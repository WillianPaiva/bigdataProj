import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class LabelKey implements WritableComparable<LabelKey>{
  private Text textKey;


  public LabelKey(){
    textKey = new Text();
  }

  public LabelKey(Text textkey){
    this.textKey = textkey;
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    textKey.readFields(arg0);
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    textKey.write(arg0);
  }

  public int size(){
    String[] s = textKey.toString().split(",");
    return s.length;
  }


  public String toString(){
    return textKey.toString();
  }
  public Text getK(){
    return this.textKey;
  }

  @Override
  public int compareTo(LabelKey o) {
    return textKey.compareTo(o.getK());
  }


}
