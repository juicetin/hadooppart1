package task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopCountryLocalityMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String sValue = value.toString();
		String[] tokens = sValue.split("\t");
		context.write(new IntWritable(Integer.parseInt(tokens[1].trim())), new Text(tokens[0].trim()));
	}
}
