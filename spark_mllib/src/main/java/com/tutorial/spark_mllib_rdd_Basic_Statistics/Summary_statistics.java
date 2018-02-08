/**
 * We provide column summary statistics for RDD[Vector],
 * which contains the column-wise max, min, mean, variance,
 * and number of nonzeros, as well as the total count.
 */
package com.tutorial.spark_mllib_rdd_Basic_Statistics;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

/**
 * @author Anubhav Jain
 *
 */
public class Summary_statistics {

	public static void main(String[] args) {
		
		/* creation of spark conf */
		SparkConf conf = new SparkConf().setAppName("Demo").setMaster("local[*]");
		/* creation of java spark context */
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		// an RDD of Vectors
		JavaRDD<Vector> mat = jsc.parallelize(
				  Arrays.asList(
				    Vectors.dense(10.0, 10.0, 100.0),
				    Vectors.dense(2.0, 20.0, 200.0),
				    Vectors.dense(6.0, 30.0, 300.0)
				  ));
		
		// Compute column summary statistics.
		MultivariateStatisticalSummary summary = Statistics.colStats(mat.rdd());
		System.out.println(summary.mean());  // a dense vector containing the mean value for each column
		//System.out.println(summary.variance());  // column-wise variance
		//System.out.println(summary.numNonzeros());  // number of nonzeros in each column

	}

}
