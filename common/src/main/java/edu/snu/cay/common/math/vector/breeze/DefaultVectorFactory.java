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
package edu.snu.cay.common.math.vector.breeze;

import breeze.storage.Zero;
import breeze.storage.Zero$;
import edu.snu.cay.common.math.vector.VectorFactory;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import javax.inject.Inject;

/**
 * Factory class for breeze based vector.
 */
public final class DefaultVectorFactory implements VectorFactory {

  // If we want to use Scala object(singleton) in Java, we should use $ sign.
  private static final ClassTag TAG = ClassTag$.MODULE$.Double();
  private static final Zero ZERO = Zero$.MODULE$.forClass(Double.TYPE);

  @Inject
  private DefaultVectorFactory() {
  }

  /**
   * Creates a dense vector with specified length.
   * @param length vector length
   * @return created vector
   */
  @Override
  public DenseVector newDenseVector(final int length) {
    return new DenseVector(breeze.linalg.DenseVector.zeros(length, TAG, ZERO));
  }

  /**
   * Creates a dense vector with given values.
   * This method does not make a deep copy of {@code data}.
   * Thus, changes in {@code data} also change the returning vector.
   * @param data elements of a vector
   * @return created vector
   */
  @Override
  public DenseVector newDenseVector(final double[] data) {
    return new DenseVector(new breeze.linalg.DenseVector(data));
  }

  /**
   * Creates a sparse vector with specified length.
   * @param length vector length
   * @return created vector
   */
  @Override
  public SparseVector newSparseVector(final int length) {
    return new SparseVector(breeze.linalg.SparseVector.zeros(length, TAG, ZERO));
  }

  /**
   * Creates a sparse vector with given indices, values, and length.
   * This method does not make a deep copy of {@code index} and {@code data}.
   * Thus, changes in {@code index} and {@code data} also change the returning vector.
   * @param index indices of vector elements
   * @param data elements of a vector
   * @param length vector length
   * @return created vector
   */
  @Override
  public SparseVector newSparseVector(final int[] index, final double[] data, final int length) {
    assert (index.length == data.length);
    return new SparseVector(new breeze.linalg.SparseVector(index, data, length, ZERO));
  }
}
