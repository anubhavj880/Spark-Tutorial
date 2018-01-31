/**
 * 
 */
package com.tutorial.spark_core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * @author Anubhav Jain
 *
 */
public class Tranformation_RDD_filter {

	public static void main(String[] args) {
		
		/* creation of spark conf */
		SparkConf conf = new SparkConf().setAppName("Demo").setMaster("local[*]");
		/* creation of java spark context */
		JavaSparkContext sc = new JavaSparkContext(conf);

		/* creation of RDD using java objects in driver program with 1 partion*/
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> rdd = sc.parallelize(data,1);
		
		/*Use of filter*/
		JavaRDD<Integer> rd = rdd.filter(new Function<Integer, Boolean>() {
			
			@Override
			public Boolean call(Integer v1) throws Exception {
				
				return (v1 % 2) == 0;
			}
		});
		
		System.out.println(rd.collect());
	}

}
