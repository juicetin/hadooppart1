package task1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LocalityMapper extends Mapper<Object, Text, Text, Text> {

	public void setup(Context context) {
	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] dataArray = value.toString().split("\t");

		// Exit when array doesn't contain all data
		if (dataArray.length < 7) {
			return;
		}

		int placeType = Integer.parseInt(dataArray[5]);

		// Leave iteration if not neighbourhood or locality
		if (placeType != 7 && placeType != 22) {
			return;
		}

		// Get locality name as appropriate
		String[] placeParts = dataArray[4].split(",");
		int offset = (placeType == 7) ? 0 : 1;

		context.write(new Text(dataArray[0]), new Text(placeParts[offset]));
	}
}
