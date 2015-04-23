package task2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountryLocalityMapper extends
Mapper<LongWritable, Text, Text, IntWritable> {
	private Hashtable<Object, List<String>> placeTable = new Hashtable<Object, List<String>>();

	public void setup(Context context) throws java.io.IOException,
	InterruptedException {

		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
			.getConfiguration());
		if (cacheFiles != null && cacheFiles.length > 0) {
			String line;
			BufferedReader placeReader = new BufferedReader(new FileReader(
				cacheFiles[0].toString()));
			try {
				while ((line = placeReader.readLine()) != null) {
					String[] parts = line.split("\t");
					if (parts.length == 7) {
						List<String> tokens = new ArrayList<String>();
						tokens.add(parts[4]);	//place-name
						tokens.add(parts[5]);	//place-type-id
						placeTable.put(parts[0], tokens);
					}
				}
			} finally {
				placeReader.close();
			}
		}
	}

	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException {

		String[] dataArray = value.toString().split("\t");
		
		// Leave iteration if entry doesn't contain all data
		if (dataArray.length != 6) {
			return;
		}
		
		String sPlaceId = dataArray[4];
		String user = dataArray[1];
		int placeId = 0;
		if (placeTable.get(sPlaceId) != null && placeTable.get(sPlaceId).get(1) != null) {
			placeId = Integer.parseInt(placeTable.get(sPlaceId).get(1));
		} else {
			return;
		}
		
		if (placeId != 7 && placeId != 22) {
			return;
		}
		
		if (placeTable.get(sPlaceId).get(0) != null) {
			String placeUrl = placeTable.get(sPlaceId).get(0);
			if (placeUrl != null) {
				if (placeUrl.split(",") != null) {
					String[] placeParts = placeUrl.split(",");
					if (placeParts.length > 1) {
						int offset = (placeId == 7) ? 0 : 1;
						String locality = placeParts[offset];
						String keyOut = placeParts[placeParts.length - 1] + "/" + locality
						+ "/" + user;
						context.write(new Text(keyOut), new IntWritable(1));
					}
				}
			}
		} else {
			return;
		}
	}
}
