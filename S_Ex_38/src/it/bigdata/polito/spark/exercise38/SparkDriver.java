package it.bigdata.polito.spark.exercise38;

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
		outputPath = args[1];
		
		SparkConf conf = new SparkConf().setAppName("Exercise #38 - Sensors with at least 2 over-threshold values");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD <String> linesRDD = sc.textFile(inputPath);
	
		JavaRDD <String> highValuesRDD = linesRDD.filter(new SelectHighValues());
		
		JavaPairRDD <String, Integer> highpairsRDD = highValuesRDD.mapToPair(new ExtractSensorIDandValue());
		
		JavaPairRDD <String, Integer> sumRDD = highpairsRDD.reduceByKey(new SelectOnlyStronglyHighSensors());
		
		JavaPairRDD <String, Integer> resultRDD = sumRDD.filter(new RemoveLowSensors());
		
		resultRDD.saveAsTextFile(outputPath);
		
		sc.close();
	}

}
