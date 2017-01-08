

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class LabelMapper 
    extends Mapper<Object, Text, LabelKey , Text>
{
  private int measureCol;
  private int labelCol;
  private int N;
  private ArrayList<Integer> labels = new ArrayList<Integer>();
  public void setup(Context context)
  {
    measureCol = Integer.parseInt(context.getConfiguration().get("measureCol"));
    labelCol = Integer.parseInt(context.getConfiguration().get("labelCol"));
    N = Integer.parseInt(context.getConfiguration().get("N"));
    for(int x = 1; x <= N; x++){
      labels.add(Integer.parseInt(context.getConfiguration().get("N"+x)));
    }
  }


  @Override
  public void map(Object key, Text value, Context context
                  ) throws IOException, InterruptedException {
    String[] values = value.toString().split(",");
    String sk = "";
    for(int x:labels){
      int l = Integer.parseInt(values[x].replaceAll("\\s+",""));
      if(sk.equals("")){
        sk += ""+l;
      }else{
        sk += ","+l;
      }
      if(values[measureCol].equals("")){
        values[measureCol] = "0";
      }
      context.write(new LabelKey(new Text(sk)),new Text(values[labelCol]+","+values[measureCol]));
    }

  }

}
