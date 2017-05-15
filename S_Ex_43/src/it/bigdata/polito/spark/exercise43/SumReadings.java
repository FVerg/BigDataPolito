package it.bigdata.polito.spark.exercise43;

import org.apache.spark.api.java.function.Function2;

public class SumReadings implements Function2<Count, Count, Count> {

	@Override
	public Count call(Count c1, Count c2) throws Exception 
	{
		return new Count (c1.numReadings+c2.numReadings, c1.numCriticalReadings+c2.numCriticalReadings);
	}

}
