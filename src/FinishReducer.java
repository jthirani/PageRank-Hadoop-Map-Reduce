package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class FinishReducer extends Reducer<DoubleWritable, Text, Text, Text> {

	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text v : values) {
			//Negates the rank to get into original form
			double rank = (-key.get());
			context.write(new Text(v.toString()), new Text(String.valueOf(rank)));
		}
	}
}