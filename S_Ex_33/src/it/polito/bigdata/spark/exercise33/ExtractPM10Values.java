package it.polito.bigdata.spark.exercise33;

import org.apache.spark.api.java.function.Function;

public class ExtractPM10Values implements Function<String, Double> {

	@Override
	public Double call(String inputLine) throws Exception 
	{
		String [] parts = inputLine.split(",");
		
		Double PM10Value;
		
		PM10Value = new Double(parts[2]);
		
		return PM10Value;
		
		
	}

}
