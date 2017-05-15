package it.bigdata.polito.it.spark.exercise42;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ExtractIDAndText implements PairFunction<String, String, String> {

	@Override
	public Tuple2<String, String> call(String inputLine) throws Exception 
	{
		String[] parts = inputLine.split(",");
		
		Tuple2<String, String> T = new Tuple2<String, String> (parts[0], parts[2]);
		
		return T;
	}

}
