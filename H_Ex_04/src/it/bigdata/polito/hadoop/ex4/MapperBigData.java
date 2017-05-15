package it.bigdata.polito.hadoop.ex4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperBigData extends Mapper<
											Text, //Input key type
											Text, 		  //Input value type
											Text, 		  //Output key type
											Text>  //Output value type
{
	private static Double threshold = new Double (50);
	
	protected void map (Text key, Text value, Context context) throws IOException, InterruptedException
	{
		// Taking the key and splitting it - Ex.: Zone1, 01-01-2010 -> fields[0]= Zone1 fields[1]=01-01-2010
		String[] fields = key.toString().split(",");
		
		String ZoneID = fields[0];
		String Date = fields[1];
		
		Double PM10level = new Double(value.toString());
		
		if (PM10level >= threshold)
			context.write(new Text(ZoneID), new Text(Date));
	}
	
}
