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

import breeze.collection.mutable.SparseArray;
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
    return new DenseVector(new breeze.linalg.DenseVector<Double>(length, TAG));
  }

  /**
   * Creates a dense vector with given values.
   * @param data elements of a vector
   * @return created vector
   */
  @Override
  public DenseVector newDenseVector(final double[] data) {
    final breeze.linalg.DenseVector dv = new breeze.linalg.DenseVector(data.length, TAG);
    for (int i = 0; i < data.length; i++) {
      dv.unsafeUpdate(i, data[i]);
    }
    return new DenseVector(dv);
  }

  /**
   * Creates a sparse vector with specified length.
   * @param length vector length
   * @return created vector
   */
  @Override
  public SparseVector newSparseVector(final int length) {
    return new SparseVector(new breeze.linalg.SparseVector(new SparseArray(length, TAG, ZERO), ZERO));
  }

  /**
   * Creates a sparse vector with given indices, values, and length.
   * @param index indices of vector elements
   * @param data elements of a vector
   * @param length vector length
   * @return created vector
   */
  @Override
  public SparseVector newSparseVector(final int[] index, final double[] data, final int length) {
    assert (index.length == data.length);
    final breeze.linalg.SparseVector sv = new breeze.linalg.SparseVector(new SparseArray(length, TAG, ZERO), ZERO);
    for (int i = 0; i < index.length; i++) {
      sv.array().update(index[i], data[i]);
    }
    return new SparseVector(sv);
  }
}
