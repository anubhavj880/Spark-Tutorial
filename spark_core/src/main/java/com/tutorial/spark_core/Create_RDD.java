package com.tutorial.spark_core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
/**
 * @author Anubhav Jain
 *
 */
public class Create_RDD {
	
	public static void main(String[] args) {
		
		/* creation of spark conf */
		SparkConf conf = new SparkConf().setAppName("Demo").setMaster("local[*]");
		/* creation of java spark context */
		JavaSparkContext sc = new JavaSparkContext(conf);

		/* creation of RDD using java objects in driver program with 2 partion*/
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> distData = sc.parallelize(data, 2);

		/* creation of RDD from text file */
		JavaRDD<String> distFile1 = sc.textFile("src/Resource/text/data1.txt");
		JavaRDD<String> distFile2 = sc.textFile("src/Resource/text");
		JavaRDD<String> distFile3 = sc.textFile("src/Resource/text/*.txt");
		JavaPairRDD<String, String> distFile4 = sc.wholeTextFiles("src/Resource/text");
		
		/*saving java RDD as serialized Java objects*/
		distFile1.saveAsObjectFile("src/Resource/output1");
		/*saving java RDD as text file */
		distFile1.saveAsTextFile("src/Resource/output2");
		
		

	}
}
