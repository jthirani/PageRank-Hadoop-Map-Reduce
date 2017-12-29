package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffReducer2 extends Reducer<DoubleWritable, Text, Text, Text> {

	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//Emits with negative rank so that they are in descending order in the file
		context.write(new Text(String.valueOf(-1.0 * key.get())), new Text(""));
	}
}