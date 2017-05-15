package it.polito.bigdata.spark.exercise30;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

public class SparkDriver 
{
	public static void main (String[] args)
	{
		String inputPath;
		String outputPath;
		
		inputPath = args[0];
		outputPath = args[1];
		
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #30");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD <String> logRDD = sc.textFile(inputPath);
		
		JavaRDD <String> googleRDD = logRDD.filter(new FilterGoogle());
		
		googleRDD.saveAsTextFile(outputPath);
		
		sc.close();
	}
}
