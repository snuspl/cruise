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
package edu.snu.cay.common.math.linalg;

/**
 * Interface for matrix whose elements are {@code Float} values.
 */
public interface Matrix {

  /**
   * Returns the number of elements.
   * @return number of elements
   */
  int size();

  /**
   * Returns the number of active elements.
   * @return number of active elements
   */
  int activeSize();

  /**
   * Returns true if the matrix is dense, false if sparse.
   * @return true if the matrix is dense, false if sparse
   */
  boolean isDense();

  /**
   * Returns the number of rows.
   * @return number of rows
   */
  int getRows();

  /**
   * Returns the number of columns.
   * @return number of columns
   */
  int getColumns();

  /**
   * Returns the element specified by the row and column indices.
   * @param rowIndex an index in range [0, rows)
   * @param columnIndex an index in range [0, columns)
   * @return element specified by given indices
   */
  Float get(int rowIndex, int columnIndex);

  /**
   * Returns the column specified by the index.
   * Modifying the return value also changes the original matrix.
   * @param index an index in range [0, columns)
   * @return a column vector specified by the given index
   */
  Vector sliceColumn(int index);

  /**
   * Returns the row specified by the index.
   * Modifying the return value also changes the original matrix.
   * @param index an index in range [0, rows)
   * @return a row vector specified by the given index
   */
  Vector sliceRow(int index);

  /**
   * Returns the columns specified by the index range.
   * Inclusive for {@code start}, exclusive for {@code end}.
   * Modifying the return value also changes the original matrix.
   * @param start an index in range [0, columns], less than or equal to {@code end}
   * @param end an index in range [0, columns], greater than or equal to {@code start}
   * @return a partial matrix which contains columns specified by the given index range
   */
  Matrix sliceColumns(int start, int end);

  /**
   * Returns the rows specified by the index range.
   * Inclusive for {@code start}, exclusive for {@code end}.
   * Modifying the return value also changes the original matrix.
   * @param start an index in range [0, rows], less than or equal to {@code end}
   * @param end an index in range [0, rows], greater than or equal to {@code start}
   * @return a partial matrix which contains rows specified by the given index range
   */
  Matrix sliceRows(int start, int end);

  /**
   * Put a column vector to the column specified by the index.
   * @param index an index in range [0, columns)
   * @param vector a column vector
   */
  void putColumn(int index, Vector vector);

  /**
   * Put a row vector to the row specified by the index.
   * @param index an index in range [0, rows)
   * @param vector a row vector
   */
  void putRow(int index, Vector vector);

  /**
   * Put a matrix to the columns specified by the index range.
   * Inclusive for {@code start}, exclusive for {@code end}.
   * @param start an index in range [0, columns], less than or equal to {@code end}
   * @param end an index in range [0, columns], greater than or equal to {@code start}
   * @param matrix a matrix with dimension (rows * (end - start))
   */
  void putColumns(int start, int end, Matrix matrix);

  /**
   * Put a matrix to the rows specified by the index range.
   * Inclusive for {@code start}, exclusive for {@code end}.
   * @param start an index in range [0, rows], less than or equal to {@code end}
   * @param end an index in range [0, rows], greater than or equal to {@code start}
   * @param matrix a matrix with dimension ((end - start) * columns)
   */
  void putRows(int start, int end, Matrix matrix);

  /**
   * Sets a matrix element.
   * @param rowIndex an index in range [0, rows)
   * @param columnIndex an index in range [0, columns)
   * @param value given value
   */
  void set(int rowIndex, int columnIndex, Float value);

  /**
   * Transpose this matrix.
   * @return transposed copy of this matrix
   */
  Matrix transpose();

  /**
   * Returns a new matrix same as this one.
   * @return a new copy of this matrix
   */
  Matrix copy();

  /**
   * Adds a scalar to all elements.
   * @param value operand scalar
   * @return operation result
   */
  Matrix add(Float value);

  /**
   * Adds a scalar to all elements (in place).
   * @param value operand scalar
   * @return operation result
   */
  Matrix addi(Float value);

  /**
   * Adds a matrix, element-wise.
   * @param matrix operand matrix
   * @return operation result
   */
  Matrix add(Matrix matrix);

  /**
   * Adds a matrix, element-wise (in place).
   * @param matrix operand matrix
   * @return operation result
   */
  Matrix addi(Matrix matrix);

  /**
   * Subtracts a scalar from all elements.
   * @param value operand scalar
   * @return operation result
   */
  Matrix sub(Float value);

  /**
   * Subtracts a scalar from all elements (in place).
   * @param value operand scalar
   * @return operation result
   */
  Matrix subi(Float value);

  /**
   * Subtracts a matrix from this matrix, element-wise.
   * @param matrix operand matrix
   * @return operation result
   */
  Matrix sub(Matrix matrix);

  /**
   * Subtracts a matrix from this matrix, element-wise (in place).
   * @param matrix operand matrix
   * @return operation result
   */
  Matrix subi(Matrix matrix);

  /**
   * Multiplies all elements by a scalar.
   * @param value operand scalar
   * @return operation result
   */
  Matrix mul(Float value);

  /**
   * Multiplies all elements by a scalar (in place).
   * @param value operand scalar
   * @return operation result
   */
  Matrix muli(Float value);

  /**
   * Multiplies this matrix by another matrix, element-wise.
   * @param matrix operand matrix
   * @return operation result
   */
  Matrix mul(Matrix matrix);

  /**
   * Multiplies this matrix by another matrix, element-wise (in place).
   * @param matrix operand matrix
   * @return operation result
   */
  Matrix muli(Matrix matrix);

  /**
   * Divides all elements by a scalar.
   * @param value operand scalar
   * @return operation result
   */
  Matrix div(Float value);

  /**
   * Divides all elements by a scalar (in place).
   * @param value operand scalar
   * @return operation result
   */
  Matrix divi(Float value);

  /**
   * Divides this matrix by another matrix, element-wise.
   * @param matrix operand matrix
   * @return operation result
   */
  Matrix div(Matrix matrix);

  /**
   * Divides this matrix by another matrix, element-wise (in place).
   * @param matrix operand matrix
   * @return operation result
   */
  Matrix divi(Matrix matrix);

  /**
   * Matrix-Vector multiplication.
   * @param vector operand vector
   * @return operation result
   */
  Vector mmul(Vector vector);
  /**
   * Matrix-Matrix multiplication.
   * @param matrix operand matrix
   * @return operation result
   */
  Matrix mmul(Matrix matrix);
}
