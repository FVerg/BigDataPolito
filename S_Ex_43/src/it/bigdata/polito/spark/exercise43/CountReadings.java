package it.bigdata.polito.spark.exercise43;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class CountReadings implements PairFunction<String, String, Count> {

	private int emptythreshold;
	
	public CountReadings(Integer threshold) 
	{
		emptythreshold = threshold;
	}

	@Override
	public Tuple2<String, Count> call(String inputLine) throws Exception 
	{
		String [] parts = inputLine.split(",");
		
		int criticalReading;
		int emptyslots = Integer.parseInt(parts[5]);
		
		if (emptyslots < emptythreshold)
			criticalReading = 1;
		else
			criticalReading = 0;
		
		Tuple2 <String, Count> T = new Tuple2 <String, Count> (parts[0], new Count (1, criticalReading));
	
		return T;
		
	}

}
