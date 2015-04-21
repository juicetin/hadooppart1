package task2;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//import com.google.common.collect.*;

public class CountryLocalityReducer extends
		Reducer<Text, IntWritable, Text, Text> {

	// private Map<String, Integer> places = new TreeMap<>();

	private Map<String, TreeMap<String, Integer>> countryLocs = new TreeMap<String, TreeMap<String, Integer>>();

	// private TreeMultimap<String,String> countryTopLocs =
	// TreeMultimap.create(Ordering.natural().reverse(), Ordering.natural());

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		String[] keyParts = key.toString().split("/");
		if (keyParts.length > 1) {
			// //Old
			// String newKey = keyParts[0] + "/" + keyParts[1];
			// int current = 0;
			// if (places.get(newKey) != null) {
			// current = places.get(newKey);
			// }
			// places.put(newKey, current += 1);

			/** Map of Country Maps of Localities **/
			// Map<String, Integer> temp = new TreeMap<>();
			// if (countryLocs.get(keyParts[0]) != null) {
			// temp.putAll(countryLocs.get(keyParts[0]));
			// // temp = a.get(keyParts[0]);
			// }

			// countryLocs.get(keyParts[0]).get(keyParts[1]);

			//Create nested TreeMap if one doesn't yet exist
			if (countryLocs.get(keyParts[0]) == null) {
				countryLocs.put(keyParts[0], new TreeMap<String, Integer>());
			}
			//Initialise value pair in nested treemap if doesn't exist yet
			if (countryLocs.get(keyParts[0]).get(keyParts[1]) == null) {
				countryLocs.get(keyParts[0]).put(keyParts[1], 0);
			}
			//Increment unique user count at locality
			int current = countryLocs.get(keyParts[0]).get(keyParts[1]);
			countryLocs.get(keyParts[0]).put(keyParts[1], current+1);
			//Limit localities to top 10 per country
			if (countryLocs.get(keyParts[0]) != null &&
					countryLocs.get(keyParts[0]).size() > 10) {
				
			}
			
//			int current = 0;
//			if (countryLocs.get(keyParts[0]) != null
//					&& countryLocs.get(keyParts[0]).get(keyParts[1]) != null) {
//				
//				countryLocs.get(keyParts[0]).put(keyParts[1], current + 1);
//			} else if (countryLocs.get(keyParts[0]).get(keyParts[1]) != null){
//				TreeMap<String, Integer> temp = new TreeMap<String, Integer>();
//				temp.put(keyParts[1], current+1);
//				countryLocs.put(keyParts[0], temp);
//			}
			// temp.put(keyParts[1], current+1);
			// countryLocs.put(keyParts[0], temp);

		}
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		// //Old
		// for (Map.Entry<String, Integer> e : places.entrySet()) {
		// context.write(new Text(e.getKey()), new IntWritable(e.getValue()));
		// }

		// Map of Coutry Maps of Localities
		for (Entry<String, TreeMap<String, Integer>> e : countryLocs.entrySet()) {
			String entry = "{";
			for (Map.Entry<String, Integer> f : e.getValue().entrySet()) {
				entry += "(" + f.getKey() + ":" + f.getValue() + ")";
			}

			context.write(new Text(e.getKey()), new Text(entry));
		}
	}
}
