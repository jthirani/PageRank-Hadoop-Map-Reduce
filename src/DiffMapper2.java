package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffMapper2 extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//Writes values with the same rank to the same reducer
		String[] parts = value.toString().split("\t");
		context.write(new DoubleWritable(-1.0 * Double.parseDouble(parts[1])), new Text(""));
	} 
}