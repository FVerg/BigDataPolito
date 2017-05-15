package it.bigdata.polito.hadoop.ex9;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverBigData extends Configured implements Tool
{
	@Override
	public int run(String[] args) throws Exception
	{
		Path inputPath;
		Path outputDir;
		
		int numberOfReducers;
		int exitCode;
		
		// Parse the parameters
		numberOfReducers = Integer.parseInt(args[0]);
		inputPath = new Path (args[1]);
		outputDir = new Path (args[2]);
		
		Configuration conf = this.getConf();
		
		// Definition of a new job
		Job job = Job.getInstance(conf);
		
		// Assigning a name to the job
		job.setJobName("Exercise #7 - Monthly total and average income");
		
		// Set input path / folder
		FileInputFormat.addInputPath(job, inputPath);
		
		// Set output folder
		FileOutputFormat.setOutputPath(job, outputDir);
		
		// Specify the Driver class
		job.setJarByClass(DriverBigData.class);
		
		// Set job input format
		job.setInputFormatClass(TextInputFormat.class);
		
		// Set job output format
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Set Mapper class
		job.setMapperClass(MapperBigData.class);
		
		// Set Map Output (Key, Value) pairs
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// Set Reducer class
		job.setReducerClass(ReducerBigData.class);
		
		// Set Reduce Output (Key, Value) pairs
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Set number of reducers
		job.setNumReduceTasks(numberOfReducers);
		
		if (job.waitForCompletion(true)==true)
			exitCode = 0;
		else
			exitCode = 1;
		
		return exitCode;
		
	}
	
	public static void main(String args[]) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);
		
		System.exit(res);
	}
}
