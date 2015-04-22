package task2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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

public class NeighbourhoodLocalityMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private Hashtable<Object, List<String>> placeTable = new Hashtable<Object, List<String>>();
	
	public void setup(Context context) throws java.io.IOException,
			InterruptedException {
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());

		if (cacheFiles != null && cacheFiles.length > 0) {
			String line;		
			//Places data for getting neighbourhoods only
			BufferedReader placeReader2 = new BufferedReader(new FileReader(
					cacheFiles[0].toString()));
			try {
				while ((line = placeReader2.readLine()) != null) {
					String[] parts = line.split("\t");
					List<String> tokens = new ArrayList<String>();
					tokens.add(parts[4]);	//place-name
					tokens.add(parts[5]);	//place-type-id
					placeTable.put(parts[0], tokens);
				}
			} finally {
				placeReader2.close();
			}
		}
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] dataArray = value.toString().split("\t");
		if (dataArray.length < 5) {
			return;
		}
		
		String sPlaceId = dataArray[4];
		int placeId = 0;
		if (placeTable.get(sPlaceId) != null) {
			placeId = Integer.parseInt(placeTable.get(sPlaceId).get(1));
		}
		
		if (placeId == 22) {
			String[] placeParts = placeTable.get(sPlaceId).get(0).split(",");
			String neighbourhood = placeParts[0];
			String locality = placeParts[1];
			String user = dataArray[1];
//			locNeighbs.put(locality + "/" + neighbourhood, 1);
			context.write(new Text(locality + "/" + neighbourhood + 
					"/" + user + "."), new IntWritable(1));
//			if (countryLocs.get(country) != null && countryLocs.get(country).containsKey(locality)) {
//				//Want to keep track of this neighbourhood in a
//				
//			}
		}
	}
}
