package task1;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;

public class ReplicateJoinReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private IntWritable valueOut = new IntWritable();
	private TreeMultimap<Integer, Text> top50Map = 
			TreeMultimap.create(Ordering.natural().reverse(), Ordering.natural());
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable val: values) {
				sum += val.get();
			}
			
			valueOut.set(sum);
			
			top50Map.put(sum, new Text(key));
			if (top50Map.size() > 50) {
				Integer smallestKey = top50Map.keySet().last();
				Text smallestValue = top50Map.get(smallestKey).last();
				top50Map.remove(smallestKey, smallestValue);
			}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (Entry<Integer, Text> entry: top50Map.entries()) {
			context.write(new Text(entry.getValue()), 
					new IntWritable (entry.getKey()));
		}
	}
}
