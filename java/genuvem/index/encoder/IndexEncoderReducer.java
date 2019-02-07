package genuvem.index.encoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import genuvem.io.IntArrayWritable;
import genuvem.io.TextIntWritable;

public class IndexEncoderReducer extends Reducer<IntWritable, TextIntWritable, IntWritable, MapWritable> {

	private Logger logger = Logger.getLogger(IndexEncoderReducer.class);

	@Override
	protected void reduce(IntWritable key, Iterable<TextIntWritable> values, Context context)
			throws IOException, InterruptedException {

		logger.debug("Start building output list at reducer for key " + key + ".");

		Map<Text, List<IntWritable>> map = new HashMap<Text, List<IntWritable>>();
		for (TextIntWritable iw : values) {

			if (!map.containsKey(iw.getText())) {
				map.put(new Text(iw.getText()), new ArrayList<IntWritable>());
			}

			List<IntWritable> list = map.get(iw.getText());
			list.add(new IntWritable(iw.getInteger().get()));
		}

		MapWritable outMap = new MapWritable();
		for (Text k : map.keySet()) {
			List<IntWritable> list = map.get(k);
			outMap.put(k, new IntArrayWritable(list.toArray(new IntWritable[list.size()])));
		}

		context.write(key, outMap);

		logger.debug("Reducer for key " + key + " has finished. Out value is " + outMap + ".");

	}

}
