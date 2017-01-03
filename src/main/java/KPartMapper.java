

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import static java.lang.System.exit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;


public class KPartMapper
    extends Mapper<Object, Text, IntWritable , Text>
{
  private ArrayList<ArrayList<Double>> listPoint = new ArrayList<ArrayList<Double>>();
  private ArrayList<Integer> colunms = new ArrayList<Integer>();
  private static Logger logger = Logger.getLogger(KPartMapper.class);
  public void setup(Context context)
  {

    int k = Integer.parseInt(context.getConfiguration().get("k"));
    Configuration conf = context.getConfiguration();

    int nbcolunms = Integer.parseInt(conf.get("nbcolunms"));

    //populate the list with the coluns used on the kmaeans 
    for(int i = 0; i< nbcolunms; i++){
      colunms.add(Integer.parseInt(conf.get("col"+i)));
    }


    try{
      FileSystem fs = FileSystem.get(conf);
      BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(conf.get("output")))));

      String line = br.readLine();

      while(line != null){
        String[] values = line.split(",");
        ArrayList<Double> temp = new ArrayList<Double>();
        for(String s:values){
          temp.add(Double.parseDouble(s));
        }
        listPoint.add(temp);
        line = br.readLine();
      }
      br.close();
    }catch(Exception e){
    } 

  }

  public IntWritable isCloserFrom(ArrayList<Double> values)
  {
    //make the firts element the result
    IntWritable result = new IntWritable(0);
    double distance = Double.MAX_VALUE;
    for(ArrayList<Double> d:listPoint){
      double a = 0;
      for(int x = 0; x < d.size();x++){
        a += Math.abs(d.get(x) - values.get(x));
      }

      if(a < distance)
      {
        distance = a;
        result = new IntWritable(listPoint.indexOf(d));
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
     ArrayList<Double> values = new ArrayList<Double>();
    for(int col:colunms){
      String svalue = value.toString().split(",")[col];
      if(svalue.isEmpty()){
        svalue = "0";
      }
      if(isDouble(svalue))
      {
        values.add(Double.parseDouble(svalue));
      }
    }
    if(!values.isEmpty()){
      IntWritable cl = isCloserFrom(values);
      context.write(cl, new Text(value.toString() + ", "+ cl.toString()));
    }
  }
  }
