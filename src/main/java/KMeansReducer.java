import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;


import org.apache.hadoop.io.Text;

public class KMeansReducer
    extends Reducer<DoubleWritable,DoubleWritable,DoubleWritable,DoubleWritable> {
    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
        int k = Integer.parseInt(context.getConfiguration().get("k"));
        double avg=0;
        double length=0;
        for(DoubleWritable value : values){
            avg += value.get();
            length++;
        }
        
        avg /= length;
        context.write(new DoubleWritable(avg), new DoubleWritable());
    }
}
