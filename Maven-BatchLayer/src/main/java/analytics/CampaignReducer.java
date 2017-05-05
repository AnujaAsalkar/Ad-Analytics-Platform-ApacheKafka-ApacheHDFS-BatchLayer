package analytics;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class CampaignReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

	//@Override
	public void reduce(Text mapper_key, Iterator<IntWritable> mapper_values,
			OutputCollector<Text, IntWritable> reducer_output, Reporter reporter)
			throws IOException {

		Text key = mapper_key;
		int frequencyForAdType= 0;
		while (mapper_values.hasNext()) {
			// replace type of value with the actual type of our value
			IntWritable value = (IntWritable) mapper_values.next();
			frequencyForAdType += value.get();
			
		}
		reducer_output.collect(key, new IntWritable(frequencyForAdType));
		
	}

}
