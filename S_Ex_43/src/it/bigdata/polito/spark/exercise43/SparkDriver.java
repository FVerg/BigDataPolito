package it.bigdata.polito.spark.exercise43;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver 
{
	public static void main (String [] args)
	{
		String inputPathReadings;
		String inputPathNeighbours;
		String outputPathPart1;
		String outputPathPart2;
		Integer threshold;
		
		inputPathReadings = args[0];
		inputPathNeighbours = args[1];
		outputPathPart1 = args[2];
		outputPathPart2 = args[3];
		threshold = new Integer(args[4]);
		
		/*
		 * Part 1: Save on a text file only the stations in a critical situation (with #FreeSlots < User Defined threshold)
		 *         which have a percentage of critical situations >80%
		 */
		
		SparkConf conf = new SparkConf ().setAppName ("Exercise #43");
		
		JavaSparkContext sc = new JavaSparkContext (conf);
		
		JavaRDD <String> readingsRDD = sc.textFile(inputPathReadings);
		
		JavaRDD <String> neighboursRDD = sc.textFile(inputPathNeighbours);
		
		JavaPairRDD <String, Count> readingsAndCriticalRDD = readingsRDD.mapToPair(new CountReadings(threshold));
		
		JavaPairRDD <String, Count> readingsCountRDD = readingsAndCriticalRDD.reduceByKey(new SumReadings());
		
		// (StationID, [NumReadings, NumCriticalReadings]
		
		JavaPairRDD <String, Double> stationAndPercentageRDD = readingsCountRDD.mapValues(new CalculatePercentage());
		//Output: StationID, Percentage
		// mapValues does not update the key, but only makes operations on the value, and returns a new value
		
		JavaPairRDD <String, Double> highPercentageStationsRDD = stationAndPercentageRDD.filter(new ExtractCriticalStations());
		
		JavaPairRDD <Double, String> invertedPairRDD = highPercentageStationsRDD.mapToPair(new Swap());
		
		JavaPairRDD <Double, String> sortedRDD = invertedPairRDD.sortByKey(false);
		
		sortedRDD.saveAsTextFile(outputPathPart1);
	}

}
