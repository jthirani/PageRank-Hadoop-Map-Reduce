package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class FinishMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] K1 = value.toString().split("\t");
		String[] K = K1[1].split(",");
		//Rank
		double rank = Double.parseDouble(K[0]);
		//negates rank so that biggest rank enters the reducer when required (reverse)
		rank = -rank;
		context.write(new DoubleWritable(rank), new Text(K1[0]));
	} 
}