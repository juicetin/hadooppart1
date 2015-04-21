package task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;

/**
 * This is a sample program to chain the place filter job and replicated join job.
 * 
 * @author Ying Zhou
 *
 */

public class JobChainDriver {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("Usage: JobChainDriver <inPlace> <inPhoto> <out> [countryName]");
			System.exit(2);
		}
		
		// pass a parameter to mapper class
//		if (otherArgs.length == 4){
//			conf.set("mapper.placeFilter.country", otherArgs[3]);
//		}
		
//		String tmpFolder = "/tmp/jtin2945";
//		Path tmpFilterOut = new Path(tmpFolder); // a temporary output path for the first job
		
//		Job placeFilterJob = new Job(conf, "Place Filter");
//		placeFilterJob.setJarByClass(LocalityMapper.class);
//		placeFilterJob.setNumReduceTasks(0);
//		placeFilterJob.setMapperClass(LocalityMapper.class);
//		placeFilterJob.setOutputKeyClass(Text.class);
//		placeFilterJob.setOutputValueClass(Text.class);
//		TextInputFormat.addInputPath(placeFilterJob, new Path(otherArgs[0]));
//		TextOutputFormat.setOutputPath(placeFilterJob, tmpFilterOut);
//		placeFilterJob.waitForCompletion(true);

		Job joinJob = new Job(conf, "Replication Join");
//		DistributedCache.addCacheFile(new Path(tmpFolder + "/part-m-00000").toUri(),joinJob.getConfiguration());
		DistributedCache.addCacheFile(new Path("/share/place.txt").toUri(),joinJob.getConfiguration());
		joinJob.setJarByClass(CountryLocalityMapper.class);
		joinJob.setNumReduceTasks(1);
		joinJob.setMapperClass(CountryLocalityMapper.class);
		joinJob.setCombinerClass(IntSumReducer.class);
		joinJob.setReducerClass(CountryLocalityReducer.class);
		joinJob.setOutputKeyClass(Text.class);
		joinJob.setOutputValueClass(IntWritable.class);
		TextInputFormat.addInputPath(joinJob, new Path(otherArgs[1]));
		TextOutputFormat.setOutputPath(joinJob, new Path(otherArgs[2]));
		joinJob.waitForCompletion(true);
//		
		
		
		// remove the temporary path
//		FileSystem.get(conf).delete(tmpFilterOut, true);

	}
}
