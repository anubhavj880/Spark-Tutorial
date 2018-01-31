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
 * @author Anubhav
 *
 */
public class Tranformation_RDD_sample {

	public static void main(String[] args) {
		
		/* creation of spark conf */
		SparkConf conf = new SparkConf().setAppName("Demo").setMaster("local[*]");
		/* creation of java spark context */
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/* creation of RDD using java objects in driver program with 2 partion*/
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
		JavaRDD<Integer> rdd = sc.parallelize(data,2);
		
		/* spark internally uses something called Bernoulli sampling for taking the sample. 
		 The fraction argument doesn't represent the fraction of the actual size of the RDD. 
		 It represent the probability of each element in the population getting selected for the sample.
		 If you set the first argument to true, then it will use something called Poisson sampling, 
		 which also results in a non-deterministic resultant sample size.
		 
		 */
		
		/*Use of sample method on rdd*/
		JavaRDD<Integer> rd = rdd.sample(true, 0.1);
		
		System.out.println(rd.collect());
	}

}
