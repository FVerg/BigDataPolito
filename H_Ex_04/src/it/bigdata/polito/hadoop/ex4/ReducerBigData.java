package it.bigdata.polito.hadoop.ex4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerBigData extends Reducer<
										      Text, 		// Input key 
										      Text,	// Input value
										      Text,			// Output key
										      Text>	// Output value
{
	@Override
	protected void reduce (Text key, Iterable<Text> dates, Context context) throws IOException, InterruptedException
	{
		String aboveThresholdDates = new String();
		
		for (Text date : dates)
		{
			if (aboveThresholdDates.length() == 0)
				aboveThresholdDates = new String(date.toString());
			else
				aboveThresholdDates.concat(date.toString());
		}
		context.write(new Text(key), new Text(aboveThresholdDates));
	}
}
