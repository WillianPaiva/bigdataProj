import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class KMeansMapper
    extends Mapper<Object, Text, DoubleWritable , DoubleWritable>
{
  private static Logger logger = Logger.getLogger(KMeansMapper.class);
  private List<DoubleWritable> listPoint;

  public void setup(Context context)
  {
    logger.info("BEGIN SETUP MAPPER");
    listPoint = new ArrayList<DoubleWritable>();
    int k = Integer.parseInt(context.getConfiguration().get("k"));
    for(int i=0;i<k;i++)
    {
      DoubleWritable kw = new DoubleWritable(Double.parseDouble(context.getConfiguration().get("k"+i)));
      logger.info("=========setup------>"+kw.toString());
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
    logger.info("BEGIN MAPPER");
    int columns = Integer.parseInt(context.getConfiguration().get("column"));
    String svalue = value.toString().split(",")[columns];
    if(svalue.isEmpty()){
      svalue = "0";
    }
    if(isDouble(svalue))
    {
      DoubleWritable kw = new DoubleWritable(Double.parseDouble(svalue));
      context.write(isCloserFrom(kw), kw);
    }
  }
}
