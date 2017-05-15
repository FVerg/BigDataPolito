package it.bigdata.polito.spark.exercise40;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class InvertKeyValue implements PairFunction<Tuple2<String, Integer>, Integer, String> {

	@Override
	public Tuple2<Integer, String> call(Tuple2<String, Integer> inputTuple) throws Exception 
	{
		Tuple2<Integer, String> T = new Tuple2<Integer, String> (inputTuple._2(), inputTuple._1());
		
		return T;

	}

}
