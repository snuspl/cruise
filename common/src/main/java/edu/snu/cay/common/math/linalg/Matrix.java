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
 * Interface for matrix whose elements are {@code double} values.
 * Linear indexing is in column-major order, unless the matrix is transposed.
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
   * Returns a element specified by the row and column indices.
   * @param rowIndex an index in range [0, rows)
   * @param columnIndex an index in range [0, columns)
   * @return element specified by given indices
   */
  double get(int rowIndex, int columnIndex);

  /**
   * Sets a matrix element.
   * @param rowIndex an index in range [0, rows)
   * @param columnIndex an index in range [0, columns)
   * @param value given value
   */
  void set(int rowIndex, int columnIndex, double value);

  /**
   * Transpose this matrix.
   * @return transposed copy of this matrix
   */
  Matrix transpose();

  /**
   * Returns a new matrix same as this one.
   * @return copied new matrix
   */
  Matrix copy();

  /**
   * Element-wise adds a matrix.
   * @param matrix operand matrix
   * @return operation result
   */
  Matrix add(Matrix matrix);

  /**
   * Element-wise adds a matrix (in place).
   * @param matrix operand matrix
   * @return operation result
   */
  Matrix addi(Matrix matrix);

  /**
   * Element-wise subtracts a matrix.
   * @param matrix operand matrix
   * @return operation result
   */
  Matrix sub(Matrix matrix);

  /**
   * Element-wise subtracts a matrix (in place).
   * @param matrix operand matrix
   * @return operation result
   */
  Matrix subi(Matrix matrix);

  /**
   * Multiplies a scalar to all elements.
   * @param value operand scalar
   * @return operation result
   */
  Matrix mul(double value);

  /**
   * Multiplies a scalar to all elements (in place).
   * @param value operand scalar
   * @return operation result
   */
  Matrix muli(double value);

  /**
   * Matrix-Matrix element-wise multiplication.
   * @param matrix operand matrix
   * @return operation result
   */
  Matrix mul(Matrix matrix);

  /**
   * Matrix-Matrix element-wise multiplication (in place).
   * @param matrix operand matrix
   * @return operation result
   */
  Matrix muli(Matrix matrix);

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
