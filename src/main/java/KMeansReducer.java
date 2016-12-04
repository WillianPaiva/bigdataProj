import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class KMeansReducer
    extends Reducer<DoubleWritable,DoubleWritable,DoubleWritable,Text> {
  private static Logger logger = Logger.getLogger(KMeansReducer.class);

    public void reduce(DoubleWritable key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
        int k = Integer.parseInt(context.getConfiguration().get("k"));
        double avg=0;
        double length=0;
        for(DoubleWritable value : values){
            avg += value.get();
            length++;
        }


        double res = avg / length;
        logger.info("======res---->"+res);
        context.write(new DoubleWritable(res), new Text(""));
    }
}
