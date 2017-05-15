package it.bigdata.polito.spark.exercise43;

public class Count 
{
	public int numReadings;
	public int numCriticalReadings;
	
	public Count (int n, int c)
	{
		numReadings = n;
		numCriticalReadings = c;
	}
	
	public String toString()
	{
		return new String ("total: " + this.numReadings + " critical: " + this.numCriticalReadings);
	}

}
