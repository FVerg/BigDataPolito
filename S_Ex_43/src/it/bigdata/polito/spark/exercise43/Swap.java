package it.bigdata.polito.spark.exercise43;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Swap implements PairFunction<Tuple2<String, Double>, Double, String> {

	@Override
	public Tuple2<Double, String> call(Tuple2<String, Double> inputTuple) throws Exception 
	{
		return new Tuple2<Double, String> (inputTuple._2(), inputTuple._1());	
	}
}
