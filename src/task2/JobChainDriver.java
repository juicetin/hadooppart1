package task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * This is a sample program to chain the place filter job and replicated join
 * job.
 * 
 * @author Ying Zhou
 *
 */

public class JobChainDriver {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err
			.println("Usage: JobChainDriver <places> <inPhoto1,2,etc...> <out>");
			System.exit(2);
		}
		


		// Easy creation of new temp paths if job scales
		int tmpFolderCount = 4;
		String[] tmpFolders = new String[tmpFolderCount + 1];
		for (int i = 1; i <= tmpFolderCount; i++) {
			tmpFolders[i] = "./tmp/" + i;
		}
		String countryAllLocalities = tmpFolders[1] + "/part-r-00000";
		String localsAllNeighbs = tmpFolders[2] + "/part-r-00000";
		String topLocalities = tmpFolders[3] + "/part-r-00000";
		String topNeighbs = tmpFolders[4] + "/part-r-00000";

		String[] INPUT_FILE = otherArgs[1].split(",");
		// String[] outFolders = new String[INPUT_FILE.length];
		// for (int i = 0; i < outFolders.length; ++i) {
		// 	outFolders[i] = otherArgs[2] + "/" + i;
		// }
		
			// Get all countries with unique users per locality
		Job locSum = new Job(conf, "a) locality unique user count");
		DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(),
			locSum.getConfiguration());
		locSum.setJarByClass(CountryLocalityMapper.class);
		locSum.setNumReduceTasks(1);
		locSum.setMapperClass(CountryLocalityMapper.class);
		locSum.setCombinerClass(IntSumReducer.class);
		locSum.setReducerClass(CountryLocalityReducer.class);
		locSum.setOutputKeyClass(Text.class);
		locSum.setOutputValueClass(IntWritable.class);
		for (String arg: INPUT_FILE)
			TextInputFormat.addInputPath(locSum, new Path(arg));
		TextOutputFormat.setOutputPath(locSum, new Path(tmpFolders[1]));
		locSum.waitForCompletion(true);

			// Get all localities with unique users per neighbourhood
		Job neighbLoc = new Job(conf, "b) neighbourhood unique user count");
		DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(),
			neighbLoc.getConfiguration());
		neighbLoc.setJarByClass(NeighbourhoodLocalityMapper.class);
		neighbLoc.setNumReduceTasks(1);
		neighbLoc.setMapperClass(NeighbourhoodLocalityMapper.class);
		neighbLoc.setCombinerClass(IntSumReducer.class);
		neighbLoc.setReducerClass(NeighbourhoodLocalityReducer.class);
		neighbLoc.setOutputKeyClass(Text.class);
		neighbLoc.setOutputValueClass(IntWritable.class);
		for (String arg: INPUT_FILE)
			TextInputFormat.addInputPath(neighbLoc, new Path(arg));
		TextOutputFormat.setOutputPath(neighbLoc, new Path(tmpFolders[2]));
		neighbLoc.waitForCompletion(true);

		Job sortLoc = new Job(conf, "c) top 10 localities per country");
		sortLoc.setJarByClass(TopCountryLocalityMapper.class);
		sortLoc.setMapperClass(TopCountryLocalityMapper.class);
		sortLoc.setNumReduceTasks(1);
		sortLoc.setReducerClass(TopCountryLocalityReducer.class);
		sortLoc.setMapOutputKeyClass(IntWritable.class);
		sortLoc.setMapOutputValueClass(Text.class);
		sortLoc.setOutputKeyClass(Text.class);
		sortLoc.setOutputValueClass(IntWritable.class);
		TextInputFormat.addInputPath(sortLoc,
			new Path(countryAllLocalities));
		TextOutputFormat.setOutputPath(sortLoc, new Path(tmpFolders[3]));
		sortLoc.waitForCompletion(true);

		Job topNei = new Job(conf, "d) top neighbourhood per locality");
		topNei.setJarByClass(TopNeighbourhoodMapper.class);
		topNei.setMapperClass(TopNeighbourhoodMapper.class);
		topNei.setNumReduceTasks(1);
		topNei.setReducerClass(TopNeighbourhoodReducer.class);
		topNei.setMapOutputKeyClass(Text.class);
		topNei.setMapOutputValueClass(Text.class);
		topNei.setOutputKeyClass(Text.class);
		topNei.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(topNei, new Path(localsAllNeighbs));
		TextOutputFormat.setOutputPath(topNei, new Path(tmpFolders[4]));
		topNei.waitForCompletion(true);

		Job finalJoin = new Job(conf, "e) join for final result");
		DistributedCache.addCacheFile(new Path(topNeighbs).toUri(),
			finalJoin.getConfiguration());
		finalJoin.setJarByClass(FinalJoinMapper.class);
		finalJoin.setMapperClass(FinalJoinMapper.class);
		finalJoin.setNumReduceTasks(1);
		finalJoin.setReducerClass(FinalJoinReducer.class);
		finalJoin.setOutputKeyClass(Text.class);
		finalJoin.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(finalJoin, new Path(topLocalities));
		TextOutputFormat.setOutputPath(finalJoin, new Path(otherArgs[2]));
		finalJoin.waitForCompletion(true);

			// // remove the temporary path
		for (int i = 1; i <= tmpFolderCount; ++i) {
			FileSystem.get(conf).delete(new Path(tmpFolders[i]), true);
		}
	}
}
