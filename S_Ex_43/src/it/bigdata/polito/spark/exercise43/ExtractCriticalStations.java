package it.bigdata.polito.spark.exercise43;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class ExtractCriticalStations implements Function<Tuple2<String, Double>, Boolean> {

	@Override
	public Boolean call(Tuple2<String, Double> inputTuple) throws Exception 
	{
		if (inputTuple._2() > 0.8)
			return true;
		else
			return false;
	}

}
