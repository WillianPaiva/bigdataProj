import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RandomPointReader extends RecordReader<IntWritable, Point2DWritable>{
	IntWritable currentKey;
	Point2DWritable currentValue;
	Random r = new Random();
	int max;
	
	public RandomPointReader() {
		super();
		this.currentKey = new IntWritable(1);
		this.currentValue = new Point2DWritable(r.nextDouble(),r.nextDouble());
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return currentKey;
	}

	@Override
	public Point2DWritable getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return currentValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return max/ ((float)currentKey.get());
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		this.max=Integer.parseInt(arg1.getConfiguration().get("points"));
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		this.currentValue = new Point2DWritable(r.nextDouble(),r.nextDouble());
		this.currentKey.set(currentKey.get()+1);
		if(currentKey.get()>max){
			return false;
		}
		return true;
	}

}
