
import static java.lang.System.exit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

/**
 * AR GS: 0: input file, 1: output file, 2: k-means, 3: n-level  n-level ,4: n-colunms
 */

public class Main {
  private static Logger logger = Logger.getLogger(Main.class);
  private static double MARGIN_OF_ERROR = 1;
  public static String KVALUES = "/values.txt";
  public static String TEMPIN = "/tempIn";
  public static String TEMPOUT = "/tempOut";
  public static String JOB_NAME = "KMeans";
  public static String SPLITTER = "\t| ";
  public static int K = 0;
  public static int N = 0;

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    K = Integer.parseInt(args[2]);
    conf.set("k",args[2]);
    int nbcolunms = args.length - 4;
    conf.set("nbcolunms",""+nbcolunms);
    N = Integer.parseInt(args[3]);
    //check if has the minimun number of arguments to run
    if(args.length < 5)
    {
      logger.error("Illegal number of argument");
      exit(0);
    }


    //delete the tmp file
    deleteFile(KVALUES, fs);

    deleteFile(TEMPIN, fs);
    deleteFile(TEMPOUT, fs);
    //copy the input file into tempin file 
    fs.mkdirs(new Path(TEMPIN+"/N1"));
    copyFile(args[0],TEMPIN+"/N1/1",fs,conf);


    kmeans(1, conf, args, fs); 
    //Get the metadata of the desired directory
    FileStatus[] fst = fs.listStatus(new Path(TEMPIN+"/N"+(N+1)));
    //Using FileUtil, getting the Paths for all the FileStatus
    Path[] pths = FileUtil.stat2Paths(fst);
    // Iterate through the directory and display the files in it

    //delete the output file if it exists
    deleteFile(args[1], fs);
    OutputStream out = fs.create(new Path(TEMPIN+"/N"+(N+1)+"/0"));
    PrintStream printStream = new PrintStream(out);
    String line="";
    printStream.println(line);
    printStream.close();
    fs.concat(new Path(TEMPIN+"/N"+(N+1)+"/0"), pths);
    copyFile(TEMPIN+"/N"+(N+1)+"/0",args[1],fs, conf);

  }

  public static boolean kmeans(int level,
                            Configuration conf,
                            String[] args,
                            FileSystem fs
                            )
      throws IllegalArgumentException,
      IOException,
      ClassNotFoundException,
      InterruptedException{


    if(level > N){
      return true;
    }
    //Get the metadata of the desired directory
    FileStatus[] fileStatus = fs.listStatus(new Path(TEMPIN+"/N"+level));
    //Using FileUtil, getting the Paths for all the FileStatus
    Path[] paths = FileUtil.stat2Paths(fileStatus);
    // Iterate through the directory and display the files in it
    for(Path path : paths)
    {
      // makes a k means for each colunm passed on the argument 
      ArrayList<ArrayList<Double>> centroids = new ArrayList<ArrayList<Double>>();
      for(int i = 4; i<args.length ; i++){
        conf.set("col"+(i-4),args[i]);
        centroids.add(calculate(conf,args[i],fs,path));
        deleteFile(KVALUES,fs);
      }

      fs.mkdirs(new Path(TEMPOUT+"/N"+level));
      String output = TEMPOUT+"/N"+level+"/KOUT"+path.getName();

      conf.unset("output");
      conf.set("output",output);
      OutputStream out = fs.create(new Path(output));
      PrintStream printStream = new PrintStream(out);
      for(int i = 0; i < K; i++){
        String line="";
        for(ArrayList<Double> d:centroids){

          if(line.equals("")){
            line = d.get(i).toString();
          }else{
            line = line+","+d.get(i).toString();
          }
        }
        printStream.println(line);
      }
      printStream.close();

      runJobBuild(path,
                  TEMPOUT+"/N"+level+"/clusters"+path.getName(),
                  KPartMapper.class,
                  KPartReducer.class,conf);
    }
    fs.mkdirs(new Path (TEMPIN+"/N"+(level+1)));
    //Get the metadata of the desired directory
    FileStatus[] fst = fs.listStatus(new Path(TEMPOUT+"/N"+level));
    //Using FileUtil, getting the Paths for all the FileStatus
    Path[] pths = FileUtil.stat2Paths(fst);
    // Iterate through the directory and display the files in it
    int count = 1;
    for(Path p: pths)
    {
      if(p.getName().contains("clusters")){
        FileStatus[] fst2 = fs.listStatus(p);
        //Using FileUtil, getting the Paths for all the FileStatus
        Path[] pt = FileUtil.stat2Paths(fst2);
        // Iterate through the directory and display the files in it
        for(Path file : pt)
        {
          if(file.getName().contains("part-r")){
            String dest = TEMPIN+"/N"+(level+1)+"/"+count;
            FileUtil.copy(fs, file,  fs, new Path(dest), false, conf);
            count++;
          }
        }
      }
    }
    return kmeans(level +1, conf, args, fs); 
    }







  ///////////////////////////////////////////////////
  // utility functions to manipulate files on hdfs //
  ///////////////////////////////////////////////////

  public static void deleteFile(String dir,FileSystem fs) throws IllegalArgumentException, IOException{
    if(fs.exists(new Path(dir))){
      /*If exist delete directory*/
      fs.delete(new Path(dir),true);
    }
  }

  public static void copyFile(String source, String dest, FileSystem fs, Configuration conf) throws IllegalArgumentException, IOException{
    if(fs.exists(new Path(source))) {
      deleteFile(dest,fs);
      FileUtil.copy(fs, new Path(source),  fs, new Path(dest), false, conf);
    }
  }


  public static ArrayList<Double> calculate(
                               Configuration conf,
                               String colunm,
                               FileSystem fs,
                               Path in
                               )
      throws IllegalArgumentException,
      IOException,
      ClassNotFoundException,
      InterruptedException{

    //basic setup
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

        Path input = in;

        getValues(conf, input, K, Integer.parseInt(colunm));

        runJob(in, KVALUES,KMeansMapper.class, KMeansReducer.class,conf);

        iter++;

      }else{
        /////////////////////////////
        // not the first iteration //
        /////////////////////////////

        //read the k-means from last iteration
        Path input = new Path(KVALUES+"/part-r-00000");


        ArrayList<Double> centers= new ArrayList<Double>();
        for(int i=0;i<K;i++)
        {
          double kw = Double.parseDouble(conf.get("k"+i));
          centers.add(kw);
        }
        getValues(conf, input, K, 0);
        //check if the k values are the same from last iteration 
        finished = true;
        for(int i=0;i<K;i++)
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

          deleteFile(KVALUES, fs);
          runJob(in, KVALUES,KMeansMapper.class, KMeansReducer.class,conf);
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
            logger.error("couldn't create the ArrayList");
          }

          // if(fs.exists(new Path(args[1]))){
          //   /*If exist delete the output path*/
          //   fs.delete(new Path(args[1]),true);
          // }
          // runJobBuild(TEMP, args[1],KPartMapper.class, KPartReducer.class,conf);
          // iter++;

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

  public static void runJob(Path in,
                            String out,
                            Class mapper,
                            Class reducer,
                            Configuration conf
                            )
      throws IOException, ClassNotFoundException, InterruptedException{
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
 
    FileInputFormat.addInputPath(job, in);
    FileOutputFormat.setOutputPath(job, new Path(out));
    job.waitForCompletion(true);
  }

  public static void runJobBuild(Path in,
                                 String out,
                                 Class mapper,
                                 Class reducer,
                                 Configuration conf
                                 )
      throws IOException, ClassNotFoundException, InterruptedException{
    Job job = Job.getInstance(conf, "Main");
    job.setNumReduceTasks(K);
    job.setJarByClass(Main.class);
    job.setMapperClass(mapper);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(reducer);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setPartitionerClass(Kpartitioner.class);
    FileInputFormat.addInputPath(job, in);
    FileOutputFormat.setOutputPath(job, new Path(out));
    job.waitForCompletion(true);
  }
}


