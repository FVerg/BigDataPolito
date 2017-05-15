package it.bigdata.polito.spark.exercise43;

import org.apache.spark.api.java.function.Function;

public class CalculatePercentage implements Function<Count, Double> {

	@Override
	public Double call(Count readings) throws Exception 
	{
		Double d =(double) readings.numReadings /(double)readings.numCriticalReadings;
		
		return d;
	}

}
