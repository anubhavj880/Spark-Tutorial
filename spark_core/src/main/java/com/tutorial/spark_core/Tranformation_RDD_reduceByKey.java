/**
 * 
 */
package com.tutorial.spark_core;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author Anubhav Jain
 *
 */
public class Tranformation_RDD_reduceByKey {

	public static void main(String[] args) {
		
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
		
		/*creation of java pair RDD*/
		JavaPairRDD<Integer, String> rdd = distFile2.mapToPair(new PairFunction<String, Integer, String>() {

			@Override
			public Tuple2<Integer, String> call(String t) throws Exception {
				
				return new Tuple2<Integer, String>(ThreadLocalRandom.current().nextInt(1, 5 + 1), t);
			}
		});
	
		/*Use of reduce by key*/
		JavaPairRDD<Integer, String> rdd1 = rdd.reduceByKey(new Function2<String, String, String>() {
			
			@Override
			public String call(String v1, String v2) throws Exception {
				
				return v1 +"==="+ v2;
			}
		});
		
		System.out.println(rdd1.collect());
	}

}
