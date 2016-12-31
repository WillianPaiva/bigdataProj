
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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
 * AR GS: 0: input file, 1: output file, 2: k-means, 3: n-level  n-level ,4: n-colunms
 */

public class Main {
  private static Logger logger = Logger.getLogger(Main.class);

  private static double MARGIN_OF_ERROR = 1;
	public static String KVALUES = "/values.txt";
	public static String JOB_NAME = "KMeans";
	public static String SPLITTER = "\t| ";
	public static List<Double> mCenters = new ArrayList<Double>();

    public static void main(String[] args) throws Exception {
      ArrayList<ArrayList<Double>> res = new ArrayList<ArrayList<Double>>();
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
        /*If exist delete the  tmp output path*/
        fs.delete(new Path(KVALUES),true);
      }
      conf.set("k",args[2]);

      // makes a k means for each colunm passed on the argument 
      for(int i = 4; i<args.length ; i++){
        res.add(calculate(args,conf,args[i],fs));
        //redelete the tmp folder to avoid error in case of a new calculation
        if(fs.exists(new Path(KVALUES))){
          /*If exist delete the  tmp output path*/
          fs.delete(new Path(KVALUES),true);
        }
      }



      //print the array list to chck the result
      int colunmres = 1;
      if(fs.exists(new Path(args[1]))){
          /*If exist delete the  tmp output path*/
          fs.delete(new Path(args[1]),true);
        }
      FSDataOutputStream out = fs.create(new Path(args[1]));
      out.writeChars("Column,k,d\n");
      for(ArrayList<Double> l:res){
        int kl = 1;
        for(Double d:l){
          logger.info("=========== colunm "+colunmres+" ============= k "+kl+" = "+d);
          kl++;
          out.writeChars(""+colunmres+","+kl+","+d+"\n");
        }
        colunmres++;
      }
    }


  public static ArrayList<Double> calculate(String[] args,
                                      Configuration conf,
                                      String colunm,
                                      FileSystem fs)
      throws IllegalArgumentException,
      IOException,
      ClassNotFoundException,
      InterruptedException{

    conf.set("column",colunm);
      int iter = 0;
      boolean finished = false;
      ArrayList<Double> result = new ArrayList<Double>();
      while(!finished){
        /////////////////////////////////////
        // iterations loop for 1 dimension //
        /////////////////////////////////////
        if(iter == 0){
          ///////////////////////////////////////////////////
          // first iteration needs to create the base file //
          ///////////////////////////////////////////////////
          Path input = new Path(args[0]);
          getValues(conf, input, Integer.parseInt(args[2]), Integer.parseInt(colunm));
          runJob(args[0], KVALUES,KMeansMapper.class, KMeansReducer.class,conf);
          iter++;
        }else{
          /////////////////////////////
          // not the first iteration //
          /////////////////////////////

          //read the k-means from last iteration
          Path input = new Path(KVALUES+"/part-r-00000");

          int k = Integer.parseInt(args[2]);

          ArrayList<Double> centers= new ArrayList<Double>();
          for(int i=0;i<k;i++)
          {
           double kw = Double.parseDouble(conf.get("k"+i));
            centers.add(kw);
          }
          getValues(conf, input, k, 0);
          //check if the k values are the same from last iteration 
          finished = true;
          for(int i=0;i<k;i++)
          {
            double a = Double.parseDouble(conf.get("k"+i));
            double b = centers.get(i);
            logger.info("========== "+a+" - "+b+" : MARGIN "+MARGIN_OF_ERROR+"==============");
            if(Math.abs(a - b) >= MARGIN_OF_ERROR){
              finished = false;
            }
          }
          if(!finished){
            //in case the not finish keep the iterations

            if(fs.exists(new Path(KVALUES))){
              /*If exist delete the output path*/
              fs.delete(new Path(KVALUES),true);
            }
            runJob(args[0], KVALUES,KMeansMapper.class, KMeansReducer.class,conf);
            iter++;

          }else{

            //create an arraylist with the result to return
            try{
              Path pt = new Path(KVALUES+"/part-r-00000");
              BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
              String line;
              line=br.readLine();

              while (line != null){
                result.add(Double.parseDouble(line));
                line=br.readLine();
              }
            }catch(Exception e){
              //doing nothing for now
            }



          }
        }
      }
      return result;
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

  public static void runJob(String in, String out, Class mapper, Class reducer, Configuration conf) throws IOException, ClassNotFoundException, InterruptedException{
    Job job = Job.getInstance(conf, "Main");
    job.setNumReduceTasks(1);
    job.setJarByClass(Main.class);
    job.setMapperClass(mapper);
    job.setMapOutputKeyClass(DoubleWritable.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setReducerClass(reducer);
    job.setOutputKeyClass(DoubleWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));
    job.waitForCompletion(true);
  }

}


