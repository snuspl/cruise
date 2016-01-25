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

import edu.snu.cay.common.math.linalg.breeze.DefaultMatrixFactory;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;

/**
 * Factory interface for {@link Matrix}.
 */
@DefaultImplementation(DefaultMatrixFactory.class)
public interface MatrixFactory {

  /**
   * Creates a dense matrix in which all elements are equal to {@code 0}.
   * @param rows number of rows
   * @param columns number of columns
   * @return a generated matrix
   */
  Matrix createDenseZeros(int rows, int columns);

  /**
   * Creates a dense matrix with the given values.
   * @param rows number of rows
   * @param columns number of columns
   * @param data elements of a matrix in column-major order
   * @return a generated matrix
   */
  Matrix createDense(int rows, int columns, double[] data);

  /**
   * Creates a CSC matrix in which all elements are equal to {@code 0}.
   * @param rows number of rows
   * @param columns number of columns
   * @param initialNonZeros number of initial nonZeros
   * @return a generated matrix
   */
  Matrix createCSCZeros(int rows, int columns, int initialNonZeros);

  /**
   * Creates a dense matrix by horizontal concatenation of vectors.
   * All vectors should have the same length.
   * @param vectors list of concatenating vectors
   * @return a generated matrix
   */
  Matrix horzcatDense(List<Vector> vectors);

  /**
   * Creates a CSC matrix by horizontal concatenation of vectors.
   * All vectors should have the same length.
   * @param vectors list of concatenating vectors
   * @return a generated matrix
   */
  Matrix horzcatSparse(List<Vector> vectors);
}
