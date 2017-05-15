package it.bigdata.polito.it.spark.exercise42;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkDriver 
{
	
	public static void main (String [] args)
	{
		String inputPathQuestions;
		String inputPathAnswers;
		String outputPath;
		
		inputPathQuestions = args[0];
		inputPathAnswers = args[1];
		outputPath = args[2];
		
		SparkConf conf = new SparkConf().setAppName ("Exercise #42");
		
		JavaSparkContext sc = new JavaSparkContext (conf);
		
		JavaRDD <String> questionsRDD = sc.textFile(inputPathQuestions);
		
		JavaRDD <String> answersRDD = sc.textFile(inputPathAnswers);
		
		JavaPairRDD <String, String> QIDAndTextRDD = questionsRDD.mapToPair(new ExtractIDAndText());
		
		//Output: (QuestionID, QuestionText)
		
		JavaPairRDD <String, String> AIDAndTextRDD = answersRDD.mapToPair(new ExtractQIDAndText());
		
		// Output: (QuestionID, AnswerText)
		
		JavaPairRDD <String, Tuple2<Iterable<String>, Iterable<String>>> resultRDD = QIDAndTextRDD.cogroup(AIDAndTextRDD);
		
		resultRDD.saveAsTextFile(outputPath);
		
		sc.close();
		
	}

}
