package it.bigdata.polito.hadoop.ex4;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperBigData extends Mapper<
											Text, //Input key type
											Text, 		  //Input value type
											Text, 		  //Output key type
											FloatWritable>  //Output value type
{
	
	protected void map (Text key, Text value, Context context) throws IOException, InterruptedException
	{
		
		String[] fields = value.toString().split(",");
		
		String SensorID = fields[0];
				
		context.write(new Text(SensorID), new FloatWritable(new Float(fields[2])));
	}
	
}
