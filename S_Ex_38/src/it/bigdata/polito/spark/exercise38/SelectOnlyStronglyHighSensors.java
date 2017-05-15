package it.bigdata.polito.spark.exercise38;

import org.apache.spark.api.java.function.Function2;

public class SelectOnlyStronglyHighSensors implements Function2<Integer, Integer, Integer> {

	@Override
	public Integer call(Integer value1, Integer value2) throws Exception 
	{
		return value1+value2;
	}

}
