import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class FakeInputSplit extends InputSplit implements Writable{
	private long length;
	
	public FakeInputSplit() {
		super();
		this.length = 1;
		// TODO Auto-generated constructor stub
	}



	public FakeInputSplit(long length) {
		super();
		this.length = length;
	}

	

	@Override
	public long getLength() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return length;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new String[]{};
	}

	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
