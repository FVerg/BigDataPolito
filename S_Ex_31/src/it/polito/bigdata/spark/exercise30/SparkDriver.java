package it.polito.bigdata.spark.exercise30;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver 
{
	public static void main (String args[])
	{
		String inputPath;
		String outputPath;
		
		inputPath = args[0];
		outputPath = args[1];
		
		SparkConf conf = new SparkConf().setAppName("Exercise #31 - Filter log file and emit only IP addresses");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD <String> logRDD = sc.textFile(inputPath);
		
		JavaRDD <String> googleRDD = logRDD.filter(new FilterGoogle());
		
		JavaRDD <String> IPRDD = googleRDD.map(new ExtractIPAddress());
		
		JavaRDD <String> DistinctIPRDD = IPRDD.distinct();
		
		DistinctIPRDD.saveAsTextFile(outputPath);
		
		sc.close();
	}
}
