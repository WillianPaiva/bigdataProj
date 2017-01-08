import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class LabelPartitioner extends Partitioner<LabelKey, Text>{

  @Override
  public int getPartition(LabelKey key, Text value, int n) {
    if(key.size() >= 1){
      return (key.size()-1);
    }else{
      return 0;
    }
  }



}
