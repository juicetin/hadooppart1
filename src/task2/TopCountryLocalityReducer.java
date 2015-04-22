package task2;

import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopCountryLocalityReducer extends
		Reducer<IntWritable, Text, Text, IntWritable> {

	Hashtable<String, Integer> countryLocCount = new Hashtable<String, Integer>();

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		for (Text val : values) {
			String country = val.toString().split("/")[0];
			if (countryLocCount.get(country) != null) {
				int current = countryLocCount.get(country);
				if (current < 10) {
					countryLocCount.put(country, current + 1);
					context.write(new Text(val.toString()),
							new IntWritable(key.get()));
				}
			} else {
				context.write(new Text(val.toString()),
						new IntWritable(key.get()));
				countryLocCount.put(val.toString().split("/")[0], 1);
			}
		}
	}
}
