package it.bigdata.polito.hadoop.ex4;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerBigData extends Reducer<
										      Text, 		// Input key 
										      FloatWritable,	// Input value
										      Text,			// Output key
										      FloatWritable>	// Output value
{
	@Override
	protected void reduce (Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException
	{
		double total = 0;
		int count = 0;
		
		for (FloatWritable value : values)
		{
			total += value.get();
			count++;
		}
		
		context.write(new Text(key), new FloatWritable(new Float(total/count)));
	}
}
