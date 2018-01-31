package com.tutorial.spark_core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * @author Anubhav Jain
 *
 */
public class Create_Pair_RDD {

	public static void main(String[] args) {
		
		/* creation of spark conf */
		SparkConf conf = new SparkConf().setAppName("Demo").setMaster("local[*]");
		/* creation of java spark context */
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/* creation of RDD from text file */
		JavaRDD<String> lines = sc.textFile("src/Resource/text/data1.txt");
		/*creation of jaba pair rdd*/
		JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2(s, 1));
		/*reduction of java pair RDD by using key*/
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
		System.out.println(counts.collect());
		

	}

}
