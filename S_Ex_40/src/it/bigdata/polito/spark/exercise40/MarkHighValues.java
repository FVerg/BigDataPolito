package it.bigdata.polito.spark.exercise40;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class MarkHighValues implements PairFunction<String, String, Integer> {

	@Override
	public Tuple2<String, Integer> call(String inputLine) throws Exception 
	{
		String [] parts = inputLine.split(",");
		
		Tuple2 <String, Integer> T = new Tuple2 (parts[0], new Integer(1));
		
		return T;
	}

}
