package it.polito.bigdata.spark.exercise33;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver 
{
	public static void main (String[] args)
	{
		String inputPath;
		
		inputPath = args[0];
		
		SparkConf conf = new SparkConf().setAppName("Exercise #33 - Top 3 PM10 values");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD <String> startingRDD = sc.textFile(inputPath);
		
		JavaRDD <Double> valuesRDD = startingRDD.map(new ExtractPM10Values());
		
		List <Double> top3Values = valuesRDD.top(3);
		
		for (Double value : top3Values)
			System.out.println(value);
		
		
	}

}
