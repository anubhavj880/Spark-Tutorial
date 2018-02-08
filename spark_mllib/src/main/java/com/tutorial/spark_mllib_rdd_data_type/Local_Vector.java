/*
 	A local vector has integer-typed and 0-based indices and double-typed values, stored on a single machine.
 */
package com.tutorial.spark_mllib_rdd_data_type;

/**
 * @author Anubhav Jain
 *
 */

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class Local_Vector {

	public static void main(String[] args) {
		
		// Create a dense vector (1.0, 0.0, 3.0).
		Vector dv = Vectors.dense(1.0, 0.0, 3.0);
		// Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
		Vector sv = Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0});
		
		System.out.println(dv);
		System.out.println(sv);

	}

}
