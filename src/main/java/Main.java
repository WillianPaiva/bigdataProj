
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * ARGS: 0: output file, 1: input file, 2: k-means, 3: columns, 4: separator between values
 * @author alaguitard
 */
public class Main {

    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	conf.set("k",args[2]);
        conf.set("column",args[3]);

        for(int i=0;i<Integer.parseInt(args[2]);i++)
            conf.set("k"+i,(Math.random()*10)+"");
        
	Job job = Job.getInstance(conf, "Main");
	job.setNumReduceTasks(1);
	job.setJarByClass(Main.class);
	job.setMapperClass(KMeansMapper.class);
	job.setMapOutputKeyClass(DoubleWritable.class);
	job.setMapOutputValueClass(DoubleWritable.class);
	job.setReducerClass(KMeansReducer.class);
	job.setOutputKeyClass(DoubleWritable.class);
	job.setOutputValueClass(DoubleWritable.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	job.setInputFormatClass(TextInputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.waitForCompletion(true);
    }
}
