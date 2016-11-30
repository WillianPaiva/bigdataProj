import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Math;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KMeansMapper
    extends Mapper<Point2DWritable, Point2DWritable,Point2DWritable , Point2DWritable>
{

    private List<Point2DWritable> listPoint;
    
    public void setup(Context context)
    {
	listPoint = new ArrayList<Point2DWritable>();
	int k = Integer.parseInt(context.getConfiguration().get("k"));
	for(int i=0;i<k;i++)
	    {
		listPoint.add(new Point2DWritable(context.getConfiguration().get("k"+i)));
	    }
    }


    public Point2Dwritable isCloserFrom(Point2DWritable value)
    {
	Point2DWritable result = listPoint.get(0);
	double distance = Double.MAX_VALUE;
	for(Point2DWritable point : listPoint)
	    {
		double a = point.getX() - value.getX();
		double b = point.getY() - value.getY();
		double tmp = Math.sqrt(a*a + b*b);
		if(tmp < distance)
		    {
			distance = tmp;
			result = point;
		    }
	    }

	
	return result;
    }
    
    public void map(IntWritable key, Point2DWritable value, Context context
		    ) throws IOException, InterruptedException {	
	context.write( isCloserFrom(value), value);
        
    }
}
