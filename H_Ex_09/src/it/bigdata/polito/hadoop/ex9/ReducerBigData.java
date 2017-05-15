package it.bigdata.polito.hadoop.ex9;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerBigData extends Reducer<
										      Text, 		// Input key 
										      IntWritable,	// Input value
										      Text,			// Output key
										      IntWritable>	// Output value
{
	@Override
	protected void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		int occurrencies = 0;
		
		for (IntWritable value : values)
			occurrencies += value.get();
		context.write(new Text(key), new IntWritable (occurrencies));
	}
}
