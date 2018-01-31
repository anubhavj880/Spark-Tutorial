/**
 * 
 */
package com.tutorial.spark_core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * @author Anubhav
 *
 */
public class Tranformation_RDD_mapPartion_with_index {

	public static void main(String[] args) {
		
		/* creation of spark conf */
		SparkConf conf = new SparkConf().setAppName("Demo").setMaster("local[*]");
		/* creation of java spark context */
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/* creation of RDD using java objects in driver program with 2 partion*/
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
		JavaRDD<Integer> rdd = sc.parallelize(data,2);
		
		/*Use of map partion with Index*/
		JavaRDD<Integer> rd = rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Integer>>() {

			@Override
			public Iterator<Integer> call(Integer v1, Iterator<Integer> v2) throws Exception {
				
				List<Integer> lt = new ArrayList<Integer>();
				while (v2.hasNext()) {
					
					lt.add(v2.next() + v1);
					
				}
				return lt.iterator();
				
			}
		}, true);
		
		
		System.out.println(rd.collect());
		

	}

}
