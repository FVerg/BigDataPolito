package it.bigdata.polito.spark.exercise38;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class RemoveLowSensors implements Function<Tuple2<String, Integer>, Boolean> {

	@Override
	public Boolean call(Tuple2<String, Integer> inputPair) throws Exception 
	{
		if (inputPair._2() < 2)
			return false;
		else 
			return true;
	}

}
