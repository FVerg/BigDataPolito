package it.polito.bigdata.spark.exercise30;

import org.apache.spark.api.java.function.Function;

public class FilterGoogle implements Function<String, Boolean> {

	@Override
	public Boolean call(String inputLine) throws Exception 
	{
		return inputLine.toLowerCase().contains("google");
	}

}
