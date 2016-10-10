/*
 * Copyright (C) 2016 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.dolphin.async.dnn.blas;

/**
 * Interface for matrix whose elements are {@code float} values.
 *
 * Linear indexing is in column-major order.
 */
public interface Matrix {

  /**
   * Returns the number of rows.
   */
  int getRows();

  /**
   * Returns the number of columns.
   */
  int getColumns();

  /**
   * Returns the element specified by the given index (linear indexing).
   */
  float get(int index);

  /**
   * Returns a column vector in which elements are specified by the given linear indices.
   * The modification on the returned vector does not affect this matrix.
   */
  Matrix get(int[] indices);

  /**
   * Returns the element specified by the row and column indices.
   */
  float get(int rowIndex, int columnIndex);

  /**
   * Sets a matrix element (linear indexing).
   */
  Matrix put(int index, float value);

  /**
   * Sets a matrix element.
   */
  Matrix put(int rowIndex, int columnIndex, float value);

  /**
   * Sets a column with the given column vector.
   */
  void putColumn(int index, Matrix vector);

  /**
   * Sets a row with the given row vector.
   */
  void putRow(int index, Matrix vector);

  /**
   * Returns a copy of a column.
   */
  Matrix getColumn(int index);

  /**
   * Returns a copy of a row.
   */
  Matrix getRow(int index);

  /**
   * Returns total number of elements.
   */
  int getLength();

  /**
   * Checks whether the matrix is a column vector.
   */
  boolean isColumnVector();

  /**
   * Checks whether the matrix is a row vector.
   */
  boolean isRowVector();

  /**
   * Sets all elements to the specified value.
   */
  Matrix fill(float value);

  /**
   * Reshapes the matrix.
   * The number of elements must not change.
   */
  Matrix reshape(int newRows, int newColumns);

  /**
   * Returns a string representation of this matrix.
   */
  String toString();

  /**
   * Converts the matrix to a one-dimensional array of {@code float}s.
   */
  float[] toFloatArray();

  /**
   * Copies {@link Matrix} {@code matrix} to this.
   * @param matrix a source matrix
   * @return this matrix.
   */
  Matrix copy(Matrix matrix);

  /**
   * Returns a duplicate of this matrix.
   */
  Matrix dup();

  /**
   * Returns transposed copy of this matrix.
   */
  Matrix transpose();


  /* Operations */

  /**
   * Adds a scalar to all elements.
   */
  Matrix add(float value);

  /**
   * Adds a scalar to all elements (in place).
   */
  Matrix addi(float value);

  /**
   * Adds a matrix, element-wise.
   */
  Matrix add(Matrix matrix);

  /**
   * Adds a matrix, element-wise (in place).
   */
  Matrix addi(Matrix matrix);

  /**
   * Adds a vector to all columns of the matrix. element-wise.
   */
  Matrix addColumnVector(Matrix vector);

  /**
   * Adds a vector to all columns of the matrix, element-wise (in place).
   */
  Matrix addiColumnVector(Matrix vector);

  /**
   * Adds a vector to all rows of the matrix, element-wise.
   */
  Matrix addRowVector(Matrix vector);

  /**
   * Adds a vector to all rows of the matrix, element-wise (in place).
   */
  Matrix addiRowVector(Matrix vector);

  /**
   * Subtracts a scalar from all elements.
   */
  Matrix sub(float value);

  /**
   * Subtracts a scalar from all elements (in place).
   */
  Matrix subi(float value);

  /**
   * Subtracts a matrix, element-wise.
   */
  Matrix sub(Matrix matrix);

  /**
   * Subtracts a matrix, element-wise (in place).
   */
  Matrix subi(Matrix matrix);

  /**
   * Subtracts a vector from all columns of the matrix, element-wise.
   */
  Matrix subColumnVector(Matrix vector);

  /**
   * Subtracts a vector from all columns of the matrix, element-wise (in place).
   */
  Matrix subiColumnVector(Matrix vector);

  /**
   * Subtracts a vector from all rows of the matrix, element-wise.
   */
  Matrix subRowVector(Matrix vector);

  /**
   * Subtracts a vector from all rows of the matrix, element-wise (in place).
   */
  Matrix subiRowVector(Matrix vector);

  /**
   * Subtracts all elements from a scalar.
   */
  Matrix rsub(float value);

  /**
   * Subtracts all elements from a scalar (in place).
   */
  Matrix rsubi(float value);

  /**
   * Subtracts from a matrix, element-wise.
   */
  Matrix rsub(Matrix matrix);

  /**
   * Subtracts from a matrix, element-wise (in place).
   */
  Matrix rsubi(Matrix matrix);

  /**
   * Multiplies all elements by a scalar.
   */
  Matrix mul(float value);

  /**
   * Multiplies all elements by a scalar (in place).
   */
  Matrix muli(float value);

  /**
   * Multiplies by a matrix, element-wise.
   */
  Matrix mul(Matrix matrix);

  /**
   * Multiplies by a matrix, element-wise (in place).
   */
  Matrix muli(Matrix matrix);

  /**
   * Multiplies all columns of the matrix by a vector, element-wise.
   */
  Matrix mulColumnVector(Matrix vector);

  /**
   * Multiplies all columns of the matrix by a vector, element-wise (in place).
   */
  Matrix muliColumnVector(Matrix vector);

  /**
   * Multiplies all rows of the matrix by a vector, element-wise.
   */
  Matrix mulRowVector(Matrix vector);

  /**
   * Multiplies all rows of the matrix by a vector, element-wise (in place).
   */
  Matrix muliRowVector(Matrix vector);

  /**
   * Divides all elements by a scalar.
   */
  Matrix div(float value);

  /**
   * Divides all elements by a scalar (in place).
   */
  Matrix divi(float value);

  /**
   * Divides by a matrix, element-wise.
   */
  Matrix div(Matrix matrix);

  /**
   * Divides by a matrix, element-wise (in place).
   */
  Matrix divi(Matrix matrix);

  /**
   * Divides all columns of the matrix by a vector, element-wise.
   */
  Matrix divColumnVector(Matrix vector);

  /**
   * Divides all columns of the matrix by a vector, element-wise (in place).
   */
  Matrix diviColumnVector(Matrix vector);

  /**
   * Divides all rows of the matrix by a vector, element-wise.
   */
  Matrix divRowVector(Matrix vector);

  /**
   * Divides a vector to all rows of the matrix by a vector, element-wise (in place).
   */
  Matrix diviRowVector(Matrix vector);

  /**
   * Divides a scalar by all elements, element-wise.
   */
  Matrix rdiv(float value);

  /**
   * Divides a scalar by all elements, element-wise (in place).
   */
  Matrix rdivi(float value);

  /**
   * Divides a matrix by this matrix, element-wise.
   */
  Matrix rdiv(Matrix matrix);

  /**
   * Divides a matrix by this matrix, element-wise (in place).
   */
  Matrix rdivi(Matrix matrix);

  /**
   * Matrix-Matrix multiplication.
   */
  Matrix mmul(Matrix matrix);

  /**
   * Matrix-Matrix multiplication (in place).
   */
  Matrix mmuli(Matrix matrix);

  /**
   * Returns result as a Matrix-Matrix multiplication.
   */
  Matrix mmul(Matrix matrix, Matrix result);

  /**
   * Multiplication between transpose of this matrix and operand matrix.
   */
  Matrix tmmul(Matrix matrix);

  /**
   * Returns result as a multiplication between transpose of this matrix and operand matrix.
   */
  Matrix tmmul(Matrix matrix, Matrix result);

  /**
   * Multiplication between this matrix and the transpose of operand matrix.
   */
  Matrix mmult(Matrix matrix);

  /**
   * Returns result as a multiplication between this matrix and the transpose of operand matrix.
   */
  Matrix mmult(Matrix matrix, Matrix result);

  /**
   * Returns the maximum element of the matrix.
   */
  float max();

  /**
   * Returns column-wise maximums.
   */
  Matrix columnMaxs();

  /**
   * Returns row-wise maximums.
   */
  Matrix rowMaxs();

  /**
   * Returns the minimum element of the matrix.
   */
  float min();

  /**
   * Returns column-wise minimums.
   */
  Matrix columnMins();

  /**
   * Returns row-wise minimums.
   */
  Matrix rowMins();

  /**
   * Returns a row vector containing the sum of elements in each column.
   */
  Matrix columnSums();

  /**
   * Returns a column vector containing the sum of elements in each row.
   */
  Matrix rowSums();

  /**
   * Returns result as a column vector containing the sum of elements in each row.
   */
  Matrix rowSums(Matrix result);

  /**
   * Returns the sum of all elements in the matrix.
   */
  float sum();

  /**
   * Compares with the size of the given matrix.
   * @param matrix the matrix to compare.
   * @return true if and only if the given matrix has the same size.
   */
  boolean hasSameSize(Matrix matrix);

  /**
   * Compares with the given matrix.
   *
   * Returns true if and only if the given matrix has the same size
   * and the maximal absolute difference in all elements is smaller than the specified tolerance.
   */
  boolean compare(Matrix matrix, float tolerance);

  /**
   * Sets values as a bernoulli matrix(in-place).
   * Takes the scale value with success probability of p, value 0 with failure probability of q = 1 − p.
   * @param prob success probability
   * @param scale value used when success
   * @return this matrix
   */
  Matrix bernoulli(float prob, float scale, long seed);
}
