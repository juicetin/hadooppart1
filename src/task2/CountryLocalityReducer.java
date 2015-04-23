package task2;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountryLocalityReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	private Map<String, Integer> places = new TreeMap<String, Integer>();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		String[] keyParts = key.toString().split("/");
		if (keyParts.length > 1) {

			// Country/Locality
			String newKey = keyParts[0] + "/" + keyParts[1];
			int current = 0;
			if (places.get(newKey) != null) {
				current = places.get(newKey);
			}
			places.put(newKey, current += 1);
		}
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		for (Map.Entry<String, Integer> e : places.entrySet()) {
			context.write(new Text(e.getKey()), new IntWritable(e.getValue()));
		}
	}
}
