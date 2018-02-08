/**
 * An IndexedRowMatrix is similar to a RowMatrix but with meaningful row indices.
 */
package com.tutorial.spark_mllib_rdd_data_type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;

/**
 * @author Anubhav Jain
 *
 */
public class Distributed_matrix_IndexedRowMatrix {

	public static void main(String[] args) {
		
		/* creation of spark conf */
		SparkConf conf = new SparkConf().setAppName("Demo").setMaster("local[*]");
		/* creation of java spark context */
		JavaSparkContext jsc = new JavaSparkContext(conf);
		// a JavaRDD of Indexed Row
		JavaRDD<IndexedRow> rows = jsc.parallelize(Arrays.asList(
				new IndexedRow(0,Vectors.dense(1.0, 0.0, 3.0)),
				new IndexedRow(1,Vectors.dense(4.0, 7.0, 3.0)),
				new IndexedRow(2,Vectors.dense(5.0, 9.0, 0.0))));
		
		// Create an IndexedRowMatrix from a JavaRDD<IndexedRow>
		IndexedRowMatrix iRMat = new IndexedRowMatrix(rows.rdd());
		
		long m = iRMat.numRows();
		long n = iRMat.numCols();
		
		
	}

}
