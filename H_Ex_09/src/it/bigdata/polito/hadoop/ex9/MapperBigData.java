package it.bigdata.polito.hadoop.ex9;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperBigData extends Mapper<
											LongWritable, //Input key type
											Text, 		  //Input value type
											Text, 		  //Output key type
											IntWritable>  //Output value type
{
	HashMap<String,Integer> wordsCounts;
	
	protected void setup (Context context)
	{
		wordsCounts = new HashMap<String, Integer> ();
	}
	
	protected void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		Integer currentFrequency;
		
		String[] words = value.toString().split("//s+");
		
		for (String word : words)
		{
			String cleanedWord = word.toLowerCase();
			
			// We get the current number of times this word has already appeared
			currentFrequency = wordsCounts.get(cleanedWord);
			
			//If it is the first time this word appears
			if (currentFrequency == 0)
				wordsCounts.put(cleanedWord,new Integer(1));
			else //Otherwise, we update the count
				wordsCounts.put(new String(cleanedWord), new Integer(currentFrequency+1));
		}
		
	}
	
	protected void cleanup (Context context) throws IOException, InterruptedException
	{
		for (Entry<String, Integer> pair : wordsCounts.entrySet())
			context.write(new Text(pair.getKey()), new IntWritable(pair.getValue()));
	}
	
}
