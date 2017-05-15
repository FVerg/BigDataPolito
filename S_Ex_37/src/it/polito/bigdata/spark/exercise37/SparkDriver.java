package it.polito.bigdata.spark.exercise37;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver 
{
	public static void main (String [] args)
	{
		String inputPath;
		String outputPath;
		
		inputPath = args[0];
		outputPath = args[1];
		
		SparkConf conf = new SparkConf().setAppName("Exercise #37 - Maximum value for each sensor");
		
		JavaSparkContext sc = new JavaSparkContext (conf);
		
		JavaRDD <String> lineRDD = sc.textFile(inputPath);
		
		JavaPairRDD <String, Double> keyvalueRDD = lineRDD.mapToPair(new ExtractKeyValue());
		
		JavaPairRDD <String, Double> maxRDD = keyvalueRDD.reduceByKey(new ExtractMaxSensors());
		
		maxRDD.saveAsTextFile(outputPath);
		
		sc.close();
	}
}
