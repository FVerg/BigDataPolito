package it.bigdata.polito.spark.exercise38;

import org.apache.spark.api.java.function.Function;

public class SelectHighValues implements Function<String, Boolean> {

	@Override
	public Boolean call(String inputLine) throws Exception 
	{
		String [] parts = inputLine.split(",");
		
		Double PM10Value = new Double(parts[2]);
		
		if (PM10Value > 50)
			return true;
		else
			return false;
	}

}
