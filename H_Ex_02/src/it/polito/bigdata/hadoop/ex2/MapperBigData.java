package it.polito.bigdata.hadoop.ex2;

import java.io.IOException;

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
	protected void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		// Split each sentence in words
		String[] words = value.toString().split("\\s+");
		
		for (String word : words)
		{
			String cleanedWord = word.toLowerCase();
			
			context.write(new Text(cleanedWord), new IntWritable(1));
		}
	}
	
}
