package task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNeighbourhoodReducer extends
		Reducer<Text, Text, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int highest = 0;
		String neighb = new String();
		for (Text val : values) {
			String[] tokens = val.toString().split("/");
			int count = Integer.parseInt(tokens[1]);
			if (count > highest) {
				neighb = tokens[0];
				highest = count;
			}
		}

		if (highest != 0) {
			context.write(new Text(key.toString() + "/" + neighb),
					new IntWritable(highest));
		}
	}
}
