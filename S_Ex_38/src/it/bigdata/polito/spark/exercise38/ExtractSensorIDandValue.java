package it.bigdata.polito.spark.exercise38;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ExtractSensorIDandValue implements PairFunction<String, String, Integer> {

	@Override
	public Tuple2<String, Integer> call(String inputLine) throws Exception 
	{
		
		String [] parts = inputLine.split(",");
		
		String SensorID = parts[0];
				
		Tuple2<String, Integer> T = new Tuple2<String,Integer> (SensorID, 1);
		
		return T;

	}

}
