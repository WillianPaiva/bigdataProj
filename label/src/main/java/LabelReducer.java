import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LabelReducer
       extends Reducer<LabelKey,Text,Text,Text> {

  public void reduce(LabelKey key, Iterable<Text> values,
                     Context context
                     ) throws IOException, InterruptedException {
    double max = 0;
    Text result = new Text();
    for(Text value : values){
      double x= Double.parseDouble(value.toString().split(",")[1]);
      if(x > max){
        max = x;
        result = new Text(key.toString()+","+value.toString());
      }
    }
    context.write(new Text(""),result);
  }
}
