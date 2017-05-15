package reduceListeners;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceListerners extends Reducer<Text, IntWritable, NullWritable, IntWritable> {

	public void reduce(IntWritable trackId, Iterable<IntWritable> userIds,
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {

		Set<Integer> userIdSet = new HashSet<Integer>();
		for (IntWritable userId : userIds) {
			userIdSet.add(userId.get());
		}
		IntWritable size = new IntWritable(userIdSet.size());
		context.write(trackId, size);
	}

}
