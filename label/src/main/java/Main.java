
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * AR GS: 0: input file, 1: output file, 2: measurecol, 3: labelcol ,4: n-colunms
 */

public class Main {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    conf.set("N",""+(args.length - 4));
    conf.set("measureCol",args[2]);
    conf.set("labelCol",args[3]);
    for(int x = 4; x < args.length ; x++){
      conf.set("N"+(x-3),args[x]);
    }

    Job job = Job.getInstance(conf, "Main");
    job.setNumReduceTasks(args.length-4);
    job.setJarByClass(Main.class);
    job.setMapperClass(LabelMapper.class);
    job.setMapOutputKeyClass(LabelKey.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(LabelReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setPartitionerClass(LabelPartitioner.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);

  }

}


