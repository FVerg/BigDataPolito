package it.polito.bigdata.spark.exercise30;

import org.apache.spark.api.java.function.Function;

public class ExtractIPAddress implements Function<String, String> {

	@Override
	public String call(String inputLine) throws Exception 
	{
		String [] parts = inputLine.split(" ");
		
		String IPAddress = parts[0];
		
		return IPAddress;
	}

}
