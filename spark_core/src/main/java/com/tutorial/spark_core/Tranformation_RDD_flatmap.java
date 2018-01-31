/**
 * 
 */
package com.tutorial.spark_core;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * @author Anubhav Jain
 *
 */
public class Tranformation_RDD_flatmap {

	public static <U> void main(String[] args) {
	
		/* creation of spark conf */
		SparkConf conf = new SparkConf().setAppName("Demo").setMaster("local[*]");
		/* creation of java spark context */
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/* creation of RDD from text file */
		JavaRDD<String> distFile1 = sc.textFile("src/Resource/text/data1.txt");
		
		/*Use of flatmap*/
		JavaRDD<String> distFile2 = distFile1.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String t) throws Exception {
				
				return Arrays.asList(t.split(" ")).iterator();
			}
		});

		System.out.println(distFile2.collect());
	}

}
