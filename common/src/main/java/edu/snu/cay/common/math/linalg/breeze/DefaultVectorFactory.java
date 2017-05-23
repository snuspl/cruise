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
package edu.snu.cay.common.math.linalg.breeze;

import breeze.math.Semiring;
import breeze.math.Semiring$;
import breeze.storage.Zero;
import breeze.storage.Zero$;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import scala.collection.JavaConversions;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import javax.inject.Inject;
import java.util.List;

/**
 * Factory class for breeze based vector.
 */
public final class DefaultVectorFactory implements VectorFactory {

  // If we want to use Scala object(singleton) in Java, we should use $ sign.
  private static final ClassTag TAG = ClassTag$.MODULE$.Float();
  private static final Zero ZERO = Zero$.MODULE$.forClass(Float.TYPE);
  private static final Semiring SEMI_RING = Semiring$.MODULE$.semiringD();

  @Inject
  private DefaultVectorFactory() {
  }

  /**
   * Creates a dense vector in which all elements are equal to {@code 0} with specified length.
   * @param length vector length
   * @return created vector
   */
  @Override
  public DenseVector createDenseZeros(final int length) {
    return new DenseVector(breeze.linalg.DenseVector.zeros(length, TAG, ZERO));
  }

  /**
   * Creates a dense vector in which all elements are equal to {@code 1} with specified length.
   * @param length vector length
   * @return created vector
   */
  @Override
  public DenseVector createDenseOnes(final int length) {
    return new DenseVector(breeze.linalg.DenseVector.ones(length, TAG, SEMI_RING));
  }

  /**
   * Creates a dense vector with given values.
   * This method does not make a deep copy of {@code data}.
   * Thus, changes in {@code data} also change the returning vector.
   * @param data elements of a vector
   * @return created vector
   */
  @Override
  public DenseVector createDense(final Float[] data) {
    return new DenseVector(new breeze.linalg.DenseVector(data));
  }

  /**
   * Creates a sparse vector in which all elements are equal to {@code 0} with specified length.
   * @param length vector length
   * @return created vector
   */
  @Override
  public SparseVector createSparseZeros(final int length) {
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
  public SparseVector createSparse(final int[] index, final Float[] data, final int length) {
    assert (index.length == data.length);
    return new SparseVector(new breeze.linalg.SparseVector(index, data, length, ZERO));
  }

  /**
   * {@inheritDoc}
   */
  public DenseVector concatDense(final List<Vector> vectors) {
    final List<breeze.linalg.DenseVector<Float>> breezeVecList = Lists.transform(vectors,
        new Function<Vector, breeze.linalg.DenseVector<Float>>() {
          public breeze.linalg.DenseVector<Float> apply(final Vector vector) {
            return ((DenseVector) vector).getBreezeVector();
          }
        });
    return new DenseVector(
        breeze.linalg.DenseVector.vertcat(JavaConversions.asScalaBuffer(breezeVecList), VectorOps.SET_DD, TAG, ZERO));
  }
}
