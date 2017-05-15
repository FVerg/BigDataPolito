package it.bigdata.polito.spark.exercise40;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver 
{
	public static void main (String[] args)
	{
		String inputPath;
		String outputPath;
		
		inputPath = args[0];
		outputPath = args [1];
		
		SparkConf conf = new SparkConf().setAppName("Exercise 40 - Sensors ordered by critical dates");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD <String> inputRDD = sc.textFile(inputPath);
		
		JavaRDD <String> highValuesRDD = inputRDD.filter(new SelectHighValues());
		
		JavaPairRDD <String, Integer> numberOfHighValuesRDD = highValuesRDD.mapToPair(new MarkHighValues());
		
		JavaPairRDD <String, Integer> numberOfSensorCriticalValuesRDD = numberOfHighValuesRDD.reduceByKey(new SumCriticalValues());
		
		JavaPairRDD <Integer, String> invertedPairRDD = numberOfSensorCriticalValuesRDD.mapToPair(new InvertKeyValue());
		
		JavaPairRDD <Integer, String> sortedPairsRDD = invertedPairRDD.sortByKey(false); //false for descending order
		
		sortedPairsRDD.saveAsTextFile(outputPath);
		
		sc.close();
		
		
	}
}
