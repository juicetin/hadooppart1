package task2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNeighbourhoodMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\t");
		int count = Integer.parseInt(tokens[1]);
		
		String[] placeToks = tokens[0].split("/");
		String locality = placeToks[0], neighb = placeToks[1];
		String outValue = neighb + "/" + count;
		context.write(new Text(locality), new Text(outValue));
	}
}
