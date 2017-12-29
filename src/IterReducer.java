package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//newRank is the value of the final computed new Rank
		double newRank = 0.0;
		//String to not lose the out edges
		String outs = "";
		for (Text v : values) {
			String val = v.toString();
			//Case for keeping track of out edges
			if (val.contains(";")) {
				outs = val;
			} 
			//Case for calculating new rank
			else {
				if (val != null && !val.equals("")) {
					newRank += Double.parseDouble(val);
				}
			}
		}
		//Computing new rank
		newRank = 0.15 + 0.85 * newRank;
		//emits the intermediate
		context.write(key, new Text(newRank + "," + outs));
	}
  
}
