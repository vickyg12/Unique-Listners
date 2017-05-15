package mapListeners;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import lab.music.Constants;
import lab.music.UniqueListeners.COUNTERS;

public class MapListerners extends Mapper<Object, Text, IntWritable, IntWritable> {

	IntWritable trackId = new IntWritable();
	IntWritable userId = new IntWritable();

	public void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {

		String[] parts = value.toString().split("[|]");
		trackId.set(Integer.parseInt(parts[Constants.TRACK_ID]));
		userId.set(Integer.parseInt(parts[Constants.USER_ID]));

		if (parts.length == 5) {
			context.write(trackId, userId);
		} else {
			/* add counter for invalid records */
			context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
		}

	}

}
