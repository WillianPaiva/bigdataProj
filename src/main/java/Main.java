import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;

public class Main {

    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	int k = Integer.parseInt(args[1]);
	conf.set("k",args[1]);
	Random r = new Random();
	List<Point2DWritable> listPoint = new ArrayList<Point2DWritable>();
	for(int i=0;i<k;i++)
	    {
		conf.set("k"+i,r.nextDouble()+";"+r.nextDouble());
		listPoint.add(new Point2DWritable(conf.get("k"+i)));
	    }
    
	Job job = Job.getInstance(conf, "Main");
	job.setNumReduceTasks(1);
	job.setJarByClass(TP3.class);
	job.setMapperClass(KMeansMapper.class);
	job.setMapOutputKeyClass(Point2DWritable.class);
	job.setMapOutputValueClass(Point2DWritbale.class);
	job.setReducerClass(KMeansReducer.class);
	job.setOutputKeyClass(Point2DWritbale.class);
	job.setOutputValueClass(Point2DWritbale.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	job.setInputFormatClass(TextInputFormat.class);
	FileOutputFormat.setOutputPath(job, new Path(args[0]));
	job.waitForCompletion(true);
	Util utility = new Util();
	while(utility.equals(listPoint,conf))
	    {
		listPoint.clear();
		for(int i=0;i<k;i++)
		    {
			listPoint.add(new Point2DWritable(conf.get("k"+i)));
		    }
		job.waitForCompletion(true);
	    }
    }
}
