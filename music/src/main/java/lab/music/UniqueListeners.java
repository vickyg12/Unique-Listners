package lab.music;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import mapListeners.MapListerners;
import reduceListeners.ReduceListerners;

public class UniqueListeners extends Configured implements Tool {

	public enum COUNTERS {
		INVALID_RECORD_COUNT
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new UniqueListeners(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			System.err.println("Usage: uniquelisteners <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(getConf(), "Unique listeners per track");
		job.setJarByClass(getClass());

		// Mapper
		job.setMapperClass(MapListerners.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Reducer
		job.setReducerClass(ReduceListerners.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// Input and output file formats
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		org.apache.hadoop.mapreduce.Counters counters = job.getCounters();
		System.out.println("No. of Invalid Records :" + counters.findCounter(COUNTERS.INVALID_RECORD_COUNT).getValue());

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub

	}

	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

}
