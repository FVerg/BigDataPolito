package it.bigdata.polito.spark.exercise41;

import org.apache.spark.api.java.function.Function;

public class SelectHighValues implements Function<String, Boolean> {

	@Override
	public Boolean call(String inputLine) throws Exception 
	{
		String [] parts = inputLine.split(",");
		
		Integer PM10Value = new Integer(parts[2]);
		
		if (PM10Value <= 50)
			return false;
		else 
			return true;
	}

}
