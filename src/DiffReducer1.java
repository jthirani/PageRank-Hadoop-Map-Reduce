package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffReducer1 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double[] ranks = new double[2];
		int count = 0;
		for (Text v : values) {
			ranks[count] = Double.parseDouble(v.toString());
			count++;
		}
		//Calculates the difference 
		double diff = Math.abs(ranks[0] - ranks[1]);
		//Emits (node, absolute difference)
		context.write(key, new Text(String.valueOf(diff)));
	}
  
}