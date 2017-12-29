package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffMapper1 extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//splits the key, value pair
		String[] K1 = value.toString().split("\t");
		
		//Splits the rank and out edges
		String[] K = K1[1].split(",");
		
		//Emits (node, rank)
		context.write(new Text(K1[0]), new Text(K[0]));
	} 
}