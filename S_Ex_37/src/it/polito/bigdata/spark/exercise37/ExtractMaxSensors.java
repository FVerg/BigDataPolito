package it.polito.bigdata.spark.exercise37;

import org.apache.spark.api.java.function.Function2;

public class ExtractMaxSensors implements Function2<Double, Double, Double> {

	@Override
	public Double call(Double value1, Double value2) throws Exception 
	{
		if (value1.compareTo(value2)>0)
			return value1;
		else
			return value2;
	}

}
