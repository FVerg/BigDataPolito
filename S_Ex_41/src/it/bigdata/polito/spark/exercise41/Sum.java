package it.bigdata.polito.spark.exercise41;

import org.apache.spark.api.java.function.Function2;

public class Sum implements Function2<Integer, Integer, Integer> {

	@Override
	public Integer call(Integer value1, Integer value2) throws Exception {
		// TODO Auto-generated method stub
		return value1+value2;
	}

}
