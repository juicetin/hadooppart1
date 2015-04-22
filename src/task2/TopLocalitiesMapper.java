package task2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;

public class TopLocalitiesMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private Map<String, TreeMultimap<Integer, String>> countryLocsSort = new TreeMap<String, TreeMultimap<Integer, String>>();
	private Map<String, TreeMultimap<String, Integer>> countryLocs = new TreeMap<String, TreeMultimap<String, Integer>>();
	private Map<String, TextIntPair> locNeighb = new Hashtable<String, TextIntPair>();
	private Hashtable<Object, List<String>> placeTable = new Hashtable<Object, List<String>>();

	public void setup(Context context) throws java.io.IOException,
			InterruptedException {
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());

		// Initialise multimaps
		Multimap<Integer, String> multimapSort = TreeMultimap.create(Ordering
				.natural().reverse(), Ordering.natural());
		for (String key : countryLocsSort.keySet()) {
			multimapSort.putAll(countryLocsSort.get(key));
		}
		Multimap<String, Integer> multimap = TreeMultimap.create(
				Ordering.natural(), Ordering.natural());
		for (String key : countryLocs.keySet()) {
			multimap.putAll(countryLocs.get(key));
		}

		if (cacheFiles != null && cacheFiles.length > 1) {
			String line;

			// Read in file with countries and their localities with photo
			// counts
			// from CountryLocalityReducer output
			BufferedReader countryReader = new BufferedReader(new FileReader(
					cacheFiles[0].toString()));
			try {
				int count = 0;
				String country = new String(), locality = new String();
				while ((line = countryReader.readLine()) != null) {
					String[] tokens = line.split(".");
					if (tokens.length > 1) {
						count = Integer.parseInt(tokens[1].trim());
						String[] placeTokens = tokens[0].split("/");
						if (placeTokens.length > 1) {
							country = placeTokens[0];
							locality = placeTokens[1];
						}
					}

					if (count != 0 && country != null && locality != null) {
						TreeMultimap<Integer, String> temp = countryLocsSort
								.get(country);
						if (countryLocsSort.get(country).size() == 10) {
							int smallestKey = temp.keySet().last();
							String smallestValue = temp.get(smallestKey).last();
							temp.remove(smallestKey, smallestValue);
						}
						// country = country.trim();
						// locality = locality.trim();
						temp.put(count, locality);
						countryLocsSort.put(country, temp);
					}

				}
			} finally {
				countryReader.close();
			}

			// Swap count and locality once top 10 obtained
			for (Entry<String, TreeMultimap<Integer, String>> countryE : countryLocsSort
					.entrySet()) {
				for (Entry<Integer, String> localityE : countryE.getValue()
						.entries()) {
					TreeMultimap<String, Integer> temp = countryLocs
							.get(countryE.getKey());
					temp.put(localityE.getValue(), localityE.getKey());
					countryLocs.put(countryE.getKey(), temp);
				}
			}

			// Get only top neighbourhoods per locality
			BufferedReader neighbReader = new BufferedReader(new FileReader(
					cacheFiles[1].toString()));
			try {
				while ((line = neighbReader.readLine()) != null) {
					String[] tokens = line.split(".");
					int count = 0;
					String[] placeParts = new String[2];
					String country = new String(), locality = new String();
					if (tokens.length > 1) {
						count = Integer.parseInt(tokens[1].trim());
						placeParts = tokens[0].split("/");
						if (placeParts.length > 1) {
							country = placeParts[0];
							locality = placeParts[1];
						}
					}

					if (locNeighb.get(country) == null) {
						TextIntPair localCount = new TextIntPair(locality,
								count);
						localCount.setKey(locality);
						locNeighb.put(country, localCount);
					} else {
						int current = locNeighb.get(country).getValue();
						if (count > current) {
							TextIntPair localCount = new TextIntPair(locality,
									count);
							locNeighb.put(country, localCount);
						}
					}
				}
			} finally {
				neighbReader.close();
			}

			// Places data for getting neighbourhoods only
			BufferedReader placeReader = new BufferedReader(new FileReader(
					cacheFiles[2].toString()));
			try {
				while ((line = placeReader.readLine()) != null) {
					String[] parts = line.split("\t");
					List<String> tokens = new ArrayList<String>();
					tokens.add(parts[4]); // place-name
					tokens.add(parts[5]); // place-type-id
					placeTable.put(parts[0], tokens);
				}
			} finally {
				placeReader.close();
			}
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] dataArray = value.toString().split("\t");
		// Leave iteration if entry doesn't contain all data
		if (dataArray.length < 5) {
			return;
		}

		String sPlaceId = dataArray[4];
		String user = dataArray[1];
		int placeId = Integer.parseInt(placeTable.get(sPlaceId).get(1));
		if (placeId != 7) {
			return;
		}

		String[] placeParts = placeTable.get(sPlaceId).get(0).split(",");
		String neighbourhood = placeParts[0];
		String locality = placeParts[1];
		String country = placeParts[placeParts.length-1];
		
		if (countryLocs.get(country).containsKey(locality)) {
			String valueOut = new String();
			valueOut += locality + ":" + countryLocs.get(country).get(locality).first();
			if (locNeighb.get(locality).getKey() != null) {
				valueOut += " " + neighbourhood + ":" + locNeighb.get(locality).getValue();
			}
			context.write(new Text(country), new Text(valueOut));
		}
	}
}
