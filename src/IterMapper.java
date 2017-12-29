package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {
	/*
	 * My intermediate - NodeID <tab> rank, all the out edge vertices separated by ';'
	 * 
	 */

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//Splits the key, value pair
		String[] K1 = value.toString().split("\t");
		//NodeID
		String nodeId = K1[0];
		//if there is rank
		if (K1[1] != null && K1[1] != "") {	
			String[] K = K1[1].split(",");
			if (K.length > 1) {
				//Rank
				String rank = K[0];
				//Splits the out edge vertices
				String[] outs = K[1].split(";");
				//Number of out edges 
				double length = outs.length;
				//Calculates the fraction for the new Rank
				double newRank = Double.parseDouble(rank) / length;
				//Emits (out edge vertex, fraction)
				for (int i = 0; i < outs.length; i++) {
					context.write(new Text(outs[i]), new Text(Double.toString(newRank)));
				}
				//Emits the (nodeID, out edge vertices)
				context.write(new Text(nodeId), new Text(K[1]));
			}
		}
		
	}
}