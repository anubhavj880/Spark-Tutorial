/**
 * A CoordinateMatrix should be used only when both dimensions of the matrix are huge and the matrix is very sparse.
 */
package com.tutorial.spark_mllib_rdd_data_type;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;

/**
 * @author Anubhav Jain
 *
 */
public class Distributed_matrix_CoordinateMatrix {

	public static void main(String[] args) {
		
		/* creation of spark conf */
		SparkConf conf = new SparkConf().setAppName("Demo").setMaster("local[*]");
		/* creation of java spark context */
		JavaSparkContext jsc = new JavaSparkContext(conf);
		// a JavaRDD of matrix entries
		JavaRDD<MatrixEntry> entries = jsc.parallelize(Arrays.asList(
										new MatrixEntry(0, 0, 1.0),
										new MatrixEntry(0, 1, 3.0),
										new MatrixEntry(1, 0, 6.0),
										new MatrixEntry(1, 1, 9.0)));
		
		// Create a CoordinateMatrix from a JavaRDD<MatrixEntry>.
		CoordinateMatrix mat = new CoordinateMatrix(entries.rdd());
		
		// Get its size.
		long m = mat.numRows();
		long n = mat.numCols();
		
		
		
	}

}
