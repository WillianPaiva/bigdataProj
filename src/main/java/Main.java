
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import static java.lang.System.exit;

/**
 * ARGS: 0: output file, 1: input file, 2: k-means, 3: columns, 4: separator between values
 * @author alaguitard
 */
public class Main {
  private static Logger logger = Logger.getLogger(Main.class);
	public static String KVALUES = "/values.txt";
	public static String JOB_NAME = "KMeans";
	public static String SPLITTER = "\t| ";
	public static List<Double> mCenters = new ArrayList<Double>();

    public static void main(String[] args) throws Exception {
      if(args.length < 5)
      {
        System.err.println("Illegal number of argument");
        exit(0);
      }
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);

      if(fs.exists(new Path(args[1]))){
        /*If exist delete the output path*/
        fs.delete(new Path(args[1]),true);
      }

      if(fs.exists(new Path(KVALUES))){
        /*If exist delete the output path*/
        fs.delete(new Path(KVALUES),true);
      }
      conf.set("k",args[2]);
      conf.set("column",args[3]);
      int iter = 0;
      boolean finished = false;
      while(!finished){
        if(iter == 0){
          Path input = new Path(args[0]);
          getValues(conf, input, Integer.parseInt(args[2]), Integer.parseInt(args[3]));

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
          FileOutputFormat.setOutputPath(job, new Path(KVALUES));
          job.waitForCompletion(true);
          iter++;

        }else{
          Path input = new Path(KVALUES+"/part-r-00000");

          int k = Integer.parseInt(args[2]);
          ArrayList<Double> centers= new ArrayList<Double>();

          for(int i=0;i<k;i++)
          {
           double kw = Double.parseDouble(conf.get("k"+i));
            centers.add(kw);
          }

          getValues(conf, input, k, 0);

          finished = true;
          for(int i=0;i<k;i++)
          {

            double a = Double.parseDouble(conf.get("k"+i));
            double b = centers.get(i);
            logger.info(a+"==============="+b);
            if(a != b){
              finished = false;
            }
          }
          if(!finished){
            if(fs.exists(new Path(KVALUES))){
              /*If exist delete the output path*/
              fs.delete(new Path(KVALUES),true);
            }
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
            FileOutputFormat.setOutputPath(job, new Path(KVALUES));
            job.waitForCompletion(true);
            iter++;
          }else{
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
            iter++;
          }


        }
      }
// 1649.1670905672895	
// 3977.2980141843973	
// 6325.872458410351	
// 8655.828378913991	
// 11474.029603919967	
// 15693.360868414085	
// 58679.348840165236	
// 394253.94670846395	
// 692453.8689320388	
// 2377288.5634674923	

    }

  public static void getValues(Configuration conf , Path file , int k, int col){
    try{

      FileSystem fs = FileSystem.get(conf);
      BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(file)));
      logger.info("==========reading==============");
      int count = 0;
      String line = br.readLine();
      while(line != null && count < k){
        String value = line.split(",")[col];
        if (!value.isEmpty())
          if (isDouble(value)) {
            conf.unset("k" + count);
            conf.set("k" + count, value);
            count++;
            logger.info("value ---->>>" + value);
          }
        line = br.readLine();
      }
      br.close();
    }catch(Exception e){
    }
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

}


