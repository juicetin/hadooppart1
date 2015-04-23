package task2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinalJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Hashtable<String, String> topNeighbs = new Hashtable<String, String>();
	
	@Override
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
					String[] tokens = line.split("\t");
					int count = Integer.parseInt(tokens[1]);
					String[] placeToks = tokens[0].split("/");
					String locality= placeToks[0];
					String neighbs = placeToks[1];
					
					topNeighbs.put(locality, neighbs + ":" + count);
				}
			} finally {
				placeReader.close();
			}
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] tokens = value.toString().split("\t");
		String[] placeToks = tokens[0].split("/");
		String country = placeToks[0];
		String locality = placeToks[1];
		int count = Integer.parseInt(tokens[1]);
		
		String outValue = locality + ":" + count;
		if (topNeighbs.get(locality) != null) {
			outValue += " " + topNeighbs.get(locality);
		} else
		{
			outValue += " :0";
		}
		
		context.write(new Text(country), new Text(outValue));
	}
}
