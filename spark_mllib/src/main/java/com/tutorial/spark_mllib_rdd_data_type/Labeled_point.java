/*
   A labeled point is a local vector, either dense or sparse, associated with a label/response.
 */
package com.tutorial.spark_mllib_rdd_data_type;

/**
 * @author Anubhav Jain
 *
 */

import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
public class Labeled_point {

	public static void main(String[] args) {
		
		/* creation of spark conf */
		SparkConf conf = new SparkConf().setAppName("Demo").setMaster("local[*]");
		/* creation of java spark context */
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		
		// Create a labeled point with a positive label and a dense feature vector.
		LabeledPoint pos = new LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0));

		// Create a labeled point with a negative label and a sparse feature vector.
		LabeledPoint neg = new LabeledPoint(0.0, Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0}));
		
		//Creation of java Rdd by reading LIBSVM format file.
		JavaRDD<LabeledPoint> examples = MLUtils.loadLibSVMFile(jsc.sc(), "src/Resource/sample_libsvm_data.txt").toJavaRDD();
			
		System.out.println(examples.collect());
		System.out.println(pos);
		System.out.println(neg);
		
	}

}
