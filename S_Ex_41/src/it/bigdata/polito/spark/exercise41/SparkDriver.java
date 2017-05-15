package it.bigdata.polito.spark.exercise41;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver 
{
	
	public static void main (String [] args)
	{
		String inputPath;
		String outputPath;
		Integer K;
		
		inputPath = args[0];
		outputPath = args[1];
		K = new Integer (args[2]);
		
		SparkConf conf = new SparkConf ().setAppName ("Exercise #41");
		
		JavaSparkContext sc = new JavaSparkContext (conf);
		
		JavaRDD <String> inputRDD = sc.textFile(inputPath);
		
		JavaRDD <String> criticalReadingsRDD = inputRDD.filter(new SelectHighValues());
		
		JavaPairRDD <String, Integer> highValuesRDD = criticalReadingsRDD.mapToPair(new PairSensorOne());
		
		JavaPairRDD <String, Integer> numberOfCriticalReadingsRDD = highValuesRDD.reduceByKey(new Sum());
		
		JavaPairRDD <Integer, String> invertedPairRDD = numberOfCriticalReadingsRDD.mapToPair(new InvertPair());
		
		JavaPairRDD <Integer, String> sortedPairRDD = invertedPairRDD.sortByKey();
		
		// ATTENTION: take returns a list of tuple2 elements
		List<Tuple2 <Integer, String>> topKCriticalSensors = sortedPairRDD.take(K);
		
		JavaPairRDD<Integer, String> topKRDD = sc.parallelizePairs(topKCriticalSensors);
		
		topKRDD.saveAsTextFile(outputPath);
		
		sc.close();
		
	}

}
