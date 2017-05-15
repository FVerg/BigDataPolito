package it.bigdata.polito.spark.exercise39;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver 
{
	public static void main(String[] args)
	{
		String inputPath;
		String outputPath;
		
		inputPath = args[0];
		outputPath = args[1];
		
		SparkConf conf = new SparkConf().setAppName("Exercise 39 - Pollution analisys");
		
		JavaSparkContext sc = new JavaSparkContext (conf);
		
		JavaRDD<String> inputLineRDD = sc.textFile(inputPath);
		
		JavaRDD<String> highSensorsRDD = inputLineRDD.filter(new SelectOverThresholdSensors());
		
		JavaPairRDD <String, String> dateAndSensorRDD = highSensorsRDD.mapToPair(new ExtractSensorAndDate());
		
		// At this point we'll have one line for each couple (SensorID, Date). We need to collapse them into a list
		JavaPairRDD <String, Iterable<String>> overThresholdDatesRDD = dateAndSensorRDD.groupByKey();
		
		overThresholdDatesRDD.saveAsTextFile(outputPath);
		
		sc.close();
	}
}
