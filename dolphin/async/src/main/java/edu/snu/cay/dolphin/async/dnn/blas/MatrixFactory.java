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
 * Factory interface for {@link Matrix}.
 */
public interface MatrixFactory {

  /**
   * Creates a column vector.
   * @param length the length of a column vector
   * @return a generated column vector
   */
  Matrix create(int length);

  /**
   * Creates a matrix.
   * @param rows the number of rows
   * @param columns the number of columns
   * @return a generated matrix
   */
  Matrix create(int rows, int columns);

  /**
   * Creates a column vector with the given values.
   * @param data elements of a column vector
   * @return a generated column vector
   */
  Matrix create(float[] data);

  /**
   * Creates a matrix with the given values.
   * @param data elements of a matrix.
   *             rows are numbered by the first index and columns are numbered by the second index.
   * @return a generated matrix
   */
  Matrix create(float[][] data);

  /**
   * Creates a matrix with the given values.
   * @param data elements of a matrix in column-major order
   * @param rows the number of rows
   * @param columns the number of columns
   * @return a generated matrix
   */
  Matrix create(float[] data, int rows, int columns);

  /**
   * Creates a column vector in which all elements are equal to {@code 1}.
   * @param length the length of a column vector
   * @return a generated column vector
   */
  Matrix ones(int length);

  /**
   * Creates a matrix in which all elements are equal to {@code 1}.
   * @param rows the number of rows
   * @param columns the number of columns
   * @return a generated matrix
   */
  Matrix ones(int rows, int columns);

  /**
   * Creates a column vector in which all elements are equal to {@code 0}.
   * @param length the length of a column vector
   * @return a generated column vector
   */
  Matrix zeros(int length);

  /**
   * Creates a matrix in which all elements are equal to {@code 0}.
   * @param rows the number of rows
   * @param columns the number of columns
   * @return a generated matrix
   */
  Matrix zeros(int rows, int columns);

  /**
   * Creates a column vector with random values uniformly distributed in 0..1.
   * @param length the length of a column vector
   * @return a generated column vector
   */
  Matrix rand(int length);

  /**
   * Creates a matrix with random values uniformly distributed in 0..1.
   * @param rows the number of rows
   * @param columns the number of columns
   * @return a generated matrix
   */
  Matrix rand(int rows, int columns);

  /**
   * Creates a matrix with random values uniformly distributed in 0..1.
   * @param rows the number of rows
   * @param columns the number of columns
   * @param seed a random seed
   * @return a generated matrix
   */
  Matrix rand(int rows, int columns, long seed);

  /**
   * Creates a column vector containing normally distributed random values with mean 0.0 and standard deviation 1.0.
   * @param length the length of a column vector
   * @return a generated column vector
   */
  Matrix randn(int length);

  /**
   * Creates a matrix containing normally distributed random values with mean 0.0 and standard deviation 1.0.
   * @param rows the number of rows
   * @param columns the number of columns
   * @return a generated matrix
   */
  Matrix randn(int rows, int columns);

  /**
   * Creates a matrix with normally distributed random values.
   * @param rows the number of rows
   * @param columns the number of columns
   * @param seed a random seed
   * @return a generated matrix
   */
  Matrix randn(int rows, int columns, long seed);

  /**
   * Concatenates two matrices horizontally.
   */
  Matrix concatHorizontally(Matrix a, Matrix b);

  /**
   * Concatenates two matrices vertically.
   */
  Matrix concatVertically(Matrix a, Matrix b);
}
