package it.bigdata.polito.hadoop.ex8;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerBigData2 extends Reducer<
										      Text, 		// Input key 
										      Text,	// Input value
										      Text,			// Output key
										      Text>	// Output value
{
	@Override
	protected void reduce (Text key, Iterable<Text> sentences, Context context) throws IOException, InterruptedException
	{
		String index = new String();
		
		for (Text ID : sentences)
		{
			if (index.length()==0)
				index = new String(ID.toString());
			else
				index.concat(ID.toString()+", ");
		}
		
		context.write(new Text(key),  new Text(index));
	}
}
