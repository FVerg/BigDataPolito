package it.bigdata.polito.spark.exercise41;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class PairSensorOne implements PairFunction<String, String, Integer> {

	@Override
	public Tuple2<String, Integer> call(String inputLine) throws Exception
	{
		Tuple2 <String, Integer> T;
		
		String [] parts = inputLine.split(",");
		
		T = new Tuple2 <String, Integer> (parts[0], new Integer(1));
		
		return T;
	}

}
