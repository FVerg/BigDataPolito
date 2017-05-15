package it.bigdata.polito.hadoop.ex8;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperBigData2 extends Mapper<
											Text, //Input key type
											Text, 		  //Input value type
											Text, 		  //Output key type
											Text>  //Output value type
{
	
	protected void map (Text key, Text value, Context context) throws IOException, InterruptedException
	{
		
		String[] fields = value.toString().split("\\s+");
		
		for (String word : fields)
		{	
			String cleanedWord = word.toLowerCase();
			if (cleanedWord.compareTo("and")!=0 && cleanedWord.compareTo("or")!=0 && cleanedWord.compareTo("not")!=0)
				context.write (new Text(cleanedWord), new Text(key));
		}
	}
	
}
