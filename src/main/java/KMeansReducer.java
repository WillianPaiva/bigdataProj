import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;


import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KMeansReducer
    extends Reducer<Point2DWritable,Point2DWritable,Point2DWritable,Point2DWritable> {
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
        int k = Integer.parseInt(context.getConfiguration().get("k"));
        
        for(IntWritable value : values){
            success += value.get();
        }
        context.write(new Text("PI"), new DoubleWritable(4*(success/drops)));
    }
}
