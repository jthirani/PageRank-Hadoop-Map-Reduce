package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String outs = "";
		for (Text v : values) {
			String[] parts = v.toString().split("\t");
			if (parts[1].equals("out")) {
				//adds the out edge vertices to the string
				outs += parts[0] + ";";
			}	
		}
		//Emits (NodeID <tab> rank, out vertices separated by ';')
		context.write(key, new Text("1," + outs));
	}
  
}
