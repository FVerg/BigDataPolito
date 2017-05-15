package it.polito.bigdata.spark.exercise37;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ExtractKeyValue implements PairFunction<String, String, Double> 
{
	public Tuple2<String, Double> call (String inputLine)
	{
		String [] parts = inputLine.split(",");
		
		Tuple2<String,Double> T = new Tuple2 <String, Double> (parts[0], new Double(parts[2]));
		
		return T;
	}
}
