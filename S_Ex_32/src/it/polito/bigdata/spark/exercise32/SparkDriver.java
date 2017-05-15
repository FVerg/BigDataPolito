package it.polito.bigdata.spark.exercise32;

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
		
		SparkConf conf = new SparkConf().setAppName("Exercise #32 - Maximum PM10 value");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD <String> linesRDD = sc.textFile(inputPath);
		
		JavaRDD <Double> PM10RDD = linesRDD.map(new ExtractPM10Values());
		
		List <Double> TopPM10 = PM10RDD.top(1);
		
		for (Double d : TopPM10)
		{
			System.out.println(d);
		}
		
		sc.close();
		
		
	}
}
