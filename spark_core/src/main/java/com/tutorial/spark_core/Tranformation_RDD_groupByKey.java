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
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author bizruntime40
 *
 */
public class Tranformation_RDD_groupByKey {

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
		
		/*Use of group by key*/
		JavaPairRDD<Integer, Iterable<String>> rdd1 =rdd.groupByKey();
		
		System.out.println(rdd1.collect());

	}
 
}
