package task2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinalJoinReducer extends Reducer <Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String topLocalities = "{";
		for (Text val: values) {
			topLocalities += "(" + val + ")" + "\t";
		}
		topLocalities = topLocalities.trim();
		topLocalities += "}";
		
		context.write(new Text(key.toString()), new Text(topLocalities));
	}
}
