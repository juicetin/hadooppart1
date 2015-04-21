package task1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReplicateJoinMapper extends Mapper<Object, Text, Text, IntWritable> {
	private Hashtable<String, String> placeTable = new Hashtable<String, String>();
//	private Text keyOut = new Text();
	private IntWritable valueOut = new IntWritable();

	public void setPlaceTable(Hashtable<String, String> place) {
		placeTable = place;
	}

	public void setup(Context context) throws java.io.IOException,
			InterruptedException {

		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());
		if (cacheFiles != null && cacheFiles.length > 0) {
			String line;
			String[] tokens;
			BufferedReader placeReader = new BufferedReader(new FileReader(
					cacheFiles[0].toString()));
			try {
				while ((line = placeReader.readLine()) != null) {
					tokens = line.split("\t");
					placeTable.put(tokens[0], tokens[1]);
				}
			} finally {
				placeReader.close();
			}
		}
	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] dataArray = value.toString().split("\t");

		// Leave iteration if entry doesn't contain all data
		if (dataArray.length < 5) {
			return;
		}

		String placeId = dataArray[4];
		String placeName = placeTable.get(placeId);
		if (placeName != null) {

//			String tags = dataArray[2];
//			String[] tagArray = tags.split(" ");
			valueOut.set(1);

			context.write(new Text(placeName), valueOut);
		}
	}
}
