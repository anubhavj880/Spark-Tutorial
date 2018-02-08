/**
 * A distributed matrix has long-typed row and column indices and double-typed values, stored distributively in one or more RDDs. 
 * We assume that the number of columns is not huge for a RowMatrix 
 */
package com.tutorial.spark_mllib_rdd_data_type;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.QRDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

/**
 * @author Anubhav Jain
 *
 */
public class Distributed_matrix_RowMatrix {

	public static void main(String[] args) {
		

		/* creation of spark conf */
		SparkConf conf = new SparkConf().setAppName("Demo").setMaster("local[*]");
		/* creation of java spark context */
		JavaSparkContext jsc = new JavaSparkContext(conf);
		// a JavaRDD of local vectors
		JavaRDD<Vector> rows = jsc.parallelize(Arrays.asList(
				Vectors.dense(1.0, 0.0, 3.0),
				Vectors.dense(4.0, 7.0, 3.0),
				Vectors.dense(5.0, 9.0, 0.0)));
		
		// Create a RowMatrix from an JavaRDD<Vector>.
		RowMatrix mat = new RowMatrix(rows.rdd());

		// Get its size.
		long m = mat.numRows();
		long n = mat.numCols();
	
		
		


	}

}
