package it.bigdata.polito.spark.exercise39;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ExtractSensorAndDate implements PairFunction<String, String, String> {

	@Override
	public Tuple2<String, String> call(String inputLine) throws Exception 
	{
		String [] parts = inputLine.split(",");
		
		String SensorID = parts[0];
		String Date = parts[1];
		
		Tuple2<String, String> T = new Tuple2 <String, String> (SensorID, Date);
		
		return T;
	}

}
