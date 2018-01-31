/**
 * 
 */
package com.tutorial.spark_core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author Anubhav Jain
 *
 */
public class Tranformation_RDD_intersection {

	public static void main(String[] args) {
	
		/* creation of spark conf */
		SparkConf conf = new SparkConf().setAppName("Demo").setMaster("local[*]");
		/* creation of java spark context */
		JavaSparkContext sc = new JavaSparkContext(conf);

		/* creation of RDD using java objects in driver program with 1 partion*/
		List<Integer> data1 = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> rdd1 = sc.parallelize(data1,1);
		
		/* creation of RDD using java objects in driver program with 1 partion*/
		List<Integer> data2 = Arrays.asList(6, 2, 3, 9, 10);
		JavaRDD<Integer> rdd2 = sc.parallelize(data2,1);
		
		/*Use of intersection*/
		JavaRDD<Integer> rdd3 = rdd2.intersection(rdd1);
		
		System.out.println(rdd3.collect());

	}

}
