/*
 * Copyright (C) 2015 Seoul National University
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

import edu.snu.cay.common.math.linalg.breeze.DefaultVectorFactory;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;

/**
 * Factory interface for {@link Vector}.
 */
@DefaultImplementation(DefaultVectorFactory.class)
public interface VectorFactory {

  /**
   * Creates a dense vector in which all elements are equal to {@code 0} with specified length.
   * @param length vector length
   * @return created vector
   */
  Vector createDenseZeros(int length);

  /**
   * Creates a dense vector in which all elements are equal to {@code 1} with specified length.
   * @param length vector length
   * @return created vector
   */
  Vector createDenseOnes(int length);

  /**
   * Creates a dense vector with given values.
   * @param data elements of a vector
   * @return created vector
   */
  Vector createDense(double[] data);

  /**
   * Creates a sparse vector in which all elements are equal to {@code 0} with specified length.
   * @param length vector length
   * @return created vector
   */
  Vector createSparseZeros(int length);

  /**
   * Creates a sparse vector with given indices, values, and length.
   * @param index indices of vector elements
   * @param data elements of a vector
   * @param length vector length
   * @return created vector
   */
  Vector createSparse(int[] index, double[] data, int length);

  /**
   * Creates a dense vector by concatenating several vectors.
   * The values are deep copies; modifying the return vector does not affect the original vectors.
   * @param vectors list of vectors to concatenate
   * @return the vector created by concatenation
   */
  Vector concatDense(List<Vector> vectors);
}
