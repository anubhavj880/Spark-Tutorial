/**
 	A local matrix has integer-typed row and column indices and double-typed values, stored on a single machine.
 */
package com.tutorial.spark_mllib_rdd_data_type;

/**
 * @author Anubhav Jain
 *
 */

import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Matrices;

public class Local_matrix {

	public static void main(String[] args) {
	
		// Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
		Matrix dm = Matrices.dense(3, 2, new double[] {1.0, 3.0, 5.0, 2.0, 4.0, 6.0});
		
		/*
		 * parameter in sparse matrix
		 * 1st - number of rows
		 * 2nd - number of columns
		 * 3rd - column index i.e it is the indexs of the elements in the array which is last parameter and
		 * last element of this array is the number of elements
		 * 4th - row index i.e it is the row number of each element in the array which is last parameter
		 * 5th - elements array.
		 * */
		
		Matrix sm = Matrices.sparse(4, 4, new int[] {0,2,3,5,6}, new int[] {0,2,1,0,3,3}, new double[] {5,3,2,7,9,1});
		System.out.println(sm);

	}

}
