package edu.upenn.nets212.hw3;

import java.io.BufferedReader;


import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SocialRankDriver 
{
  public static void main(String[] args) throws Exception 
  {
	System.out.println("Jai Thirani (jthirani)");
	  
	//Init  
	if (args[0].equals("init")) {
		init(args[1], args[2], args[3]);
		System.exit(0);
	} 
	//Iter
	else if (args[0].equals("iter")) {
		iter(args[1], args[2], args[3]);
		System.exit(0);
	}  
	//Finish
	else if (args[0].equals("finish")) {
		finish(args[1], args[2], args[3]);
		System.exit(0);
	}   
	//Diff
	else if (args[0].equals("diff")) {
		diff(args[1], args[2], args[3], args[4]);
		System.exit(0);
	}  
	//Composite
	else if (args[0].equals("composite")) {
		boolean intermediate1 = false;
		double d = 50;
		//Calls init
		init(args[1], args[3], args[6]);
		//Keeps calling iter until diff <= 30
		while(d > 0.001) {
				iter(args[3], args[4], args[6]);
				iter(args[4], args[3], args[6]);
				//iter(args[3], args[4], args[6]);
				//iter(args[4], args[3], args[6]);
				diff(args[3], args[4], args[5], args[6]); 
				d = readDiffResult(args[5]);
		}
		//Calls finish depending on which was the last intermediate
		if (intermediate1) {
			finish(args[3], args[2], "1");
		} else {
			finish(args[4], args[2], "1");
		}
		//Exits the program
		System.exit(0);
	}
	
  }
  
  public static void init(String input, String output, String numReducers) throws Exception {
	    Job job = Job.getInstance();
		job.setNumReduceTasks(Integer.parseInt(numReducers));
		job.setJarByClass(SocialRankDriver.class);
		
		// Set the paths to the input and output directory
		FileInputFormat.addInputPath(job, new Path(input));
		deleteDirectory(output);
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		// Set the Mapper and Reducer classes
		job.setMapperClass(InitMapper.class);
		job.setReducerClass(InitReducer.class);
		
		// Set the output types of the Mapper class
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// Set the output types of the Reducer class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.waitForCompletion(true);
  }
  
  public static void iter(String input, String output, String numReducers) throws Exception {
	  Job job = Job.getInstance();
		job.setNumReduceTasks(Integer.parseInt(numReducers));
		job.setJarByClass(SocialRankDriver.class);
		
		// Set the paths to the input and output directory
		FileInputFormat.addInputPath(job, new Path(input));
		deleteDirectory(output);
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		// Set the Mapper and Reducer classes
		job.setMapperClass(IterMapper.class);
		job.setReducerClass(IterReducer.class);
		
		// Set the output types of the Mapper class
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// Set the output types of the Reducer class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
  }
  
  public static void finish(String input, String output, String numReducers) throws Exception {
	  Job job = Job.getInstance();
		job.setNumReduceTasks(Integer.parseInt(numReducers));
		job.setJarByClass(SocialRankDriver.class);
		
		// Set the paths to the input and output directory
		FileInputFormat.addInputPath(job, new Path(input));
		deleteDirectory(output);
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		// Set the Mapper and Reducer classes
		job.setMapperClass(FinishMapper.class);
		job.setReducerClass(FinishReducer.class);
		
		// Set the output types of the Mapper class
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		// Set the output types of the Reducer class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.waitForCompletion(true);
  }
  
  public static void diff(String input1, String input2, String output, String numReducers) throws Exception {
	  Job job = Job.getInstance();
		job.setNumReduceTasks(Integer.parseInt(numReducers));
		job.setJarByClass(SocialRankDriver.class);
		
		// Set the paths to the input and output directory
		FileInputFormat.addInputPath(job, new Path(input1));
		FileInputFormat.addInputPath(job, new Path(input2));
		deleteDirectory("temp1231");
		FileOutputFormat.setOutputPath(job, new Path("temp1231"));
		
		// Set the Mapper and Reducer classes
		job.setMapperClass(DiffMapper1.class);
		job.setReducerClass(DiffReducer1.class);
		
		// Set the output types of the Mapper class
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// Set the output types of the Reducer class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		if (job.waitForCompletion(true)) {
		
			//final file
			
			job = Job.getInstance();
			job.setNumReduceTasks(1);
			job.setJarByClass(SocialRankDriver.class);
			
			// Set the paths to the input and output directory
			FileInputFormat.addInputPath(job, new Path("temp1231")); 
			deleteDirectory(output);
			FileOutputFormat.setOutputPath(job, new Path(output));
			
			// Set the Mapper and Reducer classes
			job.setMapperClass(DiffMapper2.class);
			job.setReducerClass(DiffReducer2.class);
			
			// Set the output types of the Mapper class
			job.setMapOutputKeyClass(DoubleWritable.class);
			job.setMapOutputValueClass(Text.class);
			
			// Set the output types of the Reducer class
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.waitForCompletion(true);
		}
  }

  // Given an output folder, returns the first double from the first part-r-00000 file
  static double readDiffResult(String path) throws Exception 
  {
    double diffnum = 0.0;
    Path diffpath = new Path(path);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(path),conf);
    
    if (fs.exists(diffpath)) {
      FileStatus[] ls = fs.listStatus(diffpath);
      for (FileStatus file : ls) {
	if (file.getPath().getName().startsWith("part-r-00000")) {
	  FSDataInputStream diffin = fs.open(file.getPath());
	  BufferedReader d = new BufferedReader(new InputStreamReader(diffin));
	  String diffcontent = d.readLine();
	  diffnum = Double.parseDouble(diffcontent);
	  d.close();
	}
      }
    }
    
    fs.close();
    return diffnum;
  }

  static void deleteDirectory(String path) throws Exception {
    Path todelete = new Path(path);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(path),conf);
    
    if (fs.exists(todelete)) 
      fs.delete(todelete, true);
      
    fs.close();
  }

}
