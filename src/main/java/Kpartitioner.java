
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

class Kpartitioner extends Partitioner<IntWritable, Text> {
  
  @Override
  public int getPartition(IntWritable key, Text value, int n) {
	return key.get();
  }

 
}
