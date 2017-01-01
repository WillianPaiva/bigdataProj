

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;


public class KPartMapper
    extends Mapper<Object, Text, IntWritable , Text>
{
  private ArrayList<DoubleWritable> listPoint;

  public void setup(Context context)
  {
    listPoint = new ArrayList<DoubleWritable>();
    int k = Integer.parseInt(context.getConfiguration().get("k"));
    for(int i=0;i<k;i++)
    {
      DoubleWritable kw = new DoubleWritable(Double.parseDouble(context.getConfiguration().get("k"+i)));
      this.listPoint.add(kw);
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

  public static boolean isDouble(String s){
    try {
      Double.parseDouble(s);
      return true;
    }
    catch (NumberFormatException ex) {
      return false;
    }
  }

  @Override
  public void map(Object key, Text value, Context context
                  ) throws IOException, InterruptedException {
   
    int columns = Integer.parseInt(context.getConfiguration().get("column"));
    String svalue = value.toString().split(",")[columns];
    if(!svalue.isEmpty() && isDouble(svalue))
    {
      DoubleWritable kw = new DoubleWritable(Double.parseDouble(svalue));
      DoubleWritable cl = isCloserFrom(kw);
      context.write(new IntWritable(listPoint.indexOf(cl)), new Text(value.toString() + ", "+ cl.toString()));
    }
  }
}
