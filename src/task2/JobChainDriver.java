package task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
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
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 1) {
			System.err.println("Usage: JobChainDriver <inPlace> <inPhoto> <out> [countryName]");
			System.exit(2);
		}
		
		String tmpFolder = "./tmp/1";	//Countries->localities
		String countryAllLocalities = tmpFolder + "/part-r-00000";
		
		String tmpFolder2 = "./tmp/2";	//Localities/neighbourhoods     
		String localsAllNeighbs = tmpFolder2 + "/part-r-00000";
		
		String tmpFolder3 = "./tmp/3";

		//Get all countries with unique users per locality
		Job locSum = new Job(conf, "Countries&Localities");
		DistributedCache.addCacheFile(new Path("/share/place.txt").toUri(),locSum.getConfiguration());
		locSum.setJarByClass(CountryLocalityMapper.class);
		locSum.setNumReduceTasks(1);
		locSum.setMapperClass(CountryLocalityMapper.class);
		locSum.setCombinerClass(IntSumReducer.class);
		locSum.setReducerClass(CountryLocalityReducer.class);
		locSum.setOutputKeyClass(Text.class);
		locSum.setOutputValueClass(IntWritable.class);
		TextInputFormat.addInputPath(locSum, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(locSum, new Path(tmpFolder));
		locSum.waitForCompletion(true);

//		
		//Get all localities with unique users per neighbourhood
		Job neighbLoc = new Job(conf, "Top neighbour to top ten localities");
//		DistributedCache.addCacheFile(new Path(countryAllLocalities).toUri(), neighbLoc.getConfiguration());
		DistributedCache.addCacheFile(new Path("/share/place.txt").toUri(),neighbLoc.getConfiguration());
		neighbLoc.setJarByClass(NeighbourhoodLocalityMapper.class);
		neighbLoc.setNumReduceTasks(1);
		neighbLoc.setMapperClass(NeighbourhoodLocalityMapper.class);
		neighbLoc.setCombinerClass(IntSumReducer.class);
		neighbLoc.setReducerClass(NeighbourhoodLocalityReducer.class);
		neighbLoc.setOutputKeyClass(Text.class);
		neighbLoc.setOutputValueClass(IntWritable.class);
		TextInputFormat.addInputPath(neighbLoc, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(neighbLoc, new Path(tmpFolder2));
		neighbLoc.waitForCompletion(true);
		
//		//Get top 10 localities per country
//		Job topLocs = new Job (conf, "Top 10 locations per country");
//		DistributedCache.addCacheFile(new Path(countryAllLocalities).toUri(), topLocs.getConfiguration());
//		DistributedCache.addCacheFile(new Path(localsAllNeighbs).toUri(), topLocs.getConfiguration());
//		DistributedCache.addCacheFile(new Path("/share/place.txt").toUri(),topLocs.getConfiguration());
//		topLocs.setJarByClass(TopLocalitiesMapper.class);
//		topLocs.setNumReduceTasks(0);
//		topLocs.setMapperClass(TopLocalitiesMapper.class);
//		topLocs.setReducerClass(TopLocalitiesReducer.class);
//		topLocs.setOutputKeyClass(Text.class);
//		topLocs.setOutputValueClass(Text.class);
//		TextInputFormat.addInputPath(topLocs, new Path(countryAllLocalities));
//		TextOutputFormat.setOutputPath(topLocs, new Path(tmpFolder3));
//		topLocs.waitForCompletion(true);
		
		// remove the temporary path
//		FileSystem.get(conf).delete(new Path(tmpFolder), true);
//		FileSystem.get(conf).delete(new Path(tmpFolder2), true);

	}
}
