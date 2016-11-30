import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class KMeansMapper
    extends Mapper<Text, Text, DoubleWritable , DoubleWritable>
{
    private List<DoubleWritable> listPoint;
    
    public void setup(Context context)
    {
	listPoint = new ArrayList<DoubleWritable>();
	int k = Integer.parseInt(context.getConfiguration().get("k"));
	for(int i=0;i<k;i++)
	    {
                DoubleWritable kw = new DoubleWritable(Double.parseDouble(context.getConfiguration().get("k"+i)));
		listPoint.add(kw);
	    }
    }


    public DoubleWritable isCloserFrom(DoubleWritable value)
    {
	DoubleWritable result = listPoint.get(0);
	double distance = Double.MAX_VALUE;
	for(DoubleWritable point : listPoint)
	    {
		double a = point.get() - value.get();
		double tmp = Math.abs(a);
		if(tmp < distance)
		    {
			distance = tmp;
			result = point;
		    }
	    }
	return result;
    }
    
    @Override
    public void map(Text key, Text value, Context context
		    ) throws IOException, InterruptedException {	
        int columns = Integer.parseInt(context.getConfiguration().get("columns"));
        String svalue = value.toString().split(",")[columns];
        if(!svalue.isEmpty())
        {
            DoubleWritable kw = new DoubleWritable(Double.parseDouble(svalue));
            context.write(isCloserFrom(kw), kw);
        }
    }
}
