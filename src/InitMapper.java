package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//gets the edges
		String line = value.toString();
		//parses the vertices
		String[] vertices = line.split("\t");
		//Emits vertices in both permutations so that even if a node has no 
		//out edge vertices it will be there in the first intermediate
		context.write(new Text(vertices[0]), new Text(vertices[1] + "\tout"));
		context.write(new Text(vertices[1]), new Text(vertices[0] + "\tin"));
	}
}