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

import breeze.linalg.NumericOps;
import breeze.linalg.package$;
import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorEntry;
import org.apache.commons.lang.NotImplementedException;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.util.Iterator;

/**
 * Vector implementation based on breeze sparse vector.
 * This class should be initialized by {@link edu.snu.cay.common.math.linalg.VectorFactory}.
 */
public class SparseVector implements Vector {

  private final breeze.linalg.SparseVector<Float> breezeVector;

  SparseVector(final breeze.linalg.SparseVector<Float> breezeVector) {
    this.breezeVector = breezeVector;
  }

  breeze.linalg.SparseVector<Float> getBreezeVector() {
    return breezeVector;
  }

  /**
   * Return the number of elements in vector.
   * @return number of elements
   */
  @Override
  public int length() {
    return breezeVector.length();
  }

  /**
   * Return the number of active(nonzero) elements.
   * @return number of active elements
   */
  @Override
  public int activeSize() {
    return breezeVector.activeSize();
  }

  /**
   * Returns true if the vector is dense, false if sparse.
   * @return false
   */
  @Override
  public boolean isDense() {
    return false;
  }

  /**
   * Gets an element specified by given index.
   * @param index an index in range [0, length)
   * @return element specified by given index
   */
  @Override
  public float get(final int index) {
    return breezeVector.array().apply(index);
  }

  @Override
  public Vector slice(final int start, final int end) {
    throw new NotImplementedException();
  }

  /**
   * Sets an element to given value.
   * @param index an index in range [0, length)
   * @param value value to be set
   */
  @Override
  public void set(final int index, final float value) {
    breezeVector.array().update(index, value);
  }

  @Override
  public boolean equals(final Object o) {
    if (o instanceof SparseVector) {
      return breezeVector.equals(((SparseVector) o).breezeVector);
    }
    if (o instanceof DenseVector) {
      return breezeVector.equals(((DenseVector) o).getBreezeVector());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return breezeVector.hashCode();
  }

  /**
   * Returns a new vector same as this one.
   * @return copied new vector
   */
  @Override
  public SparseVector copy() {
    return new SparseVector(breezeVector.copy());
  }

  @Override
  public Iterator<VectorEntry> iterator() {
    return new SparseVectorIterator();
  }

  @Override
  public String toString() {
    return breezeVector.toString();
  }

  /**
   * Adds a scalar to all elements of this vector (in place).
   * @param value operand scalar
   * @return operation result
   */
  @Override
  public Vector addi(final float value) {
    ((NumericOps)breezeVector).$plus$eq(value, VectorOps.ADDI_ST);
    return this;
  }

  /**
   * Adds a scalar to all elements of this vector.
   * @param value operand scalar
   * @return operation result
   */
  @Override
  public Vector add(final float value) {
    return new SparseVector((breeze.linalg.SparseVector<Float>) breezeVector.$plus(value, VectorOps.ADD_ST));
  }

  /**
   * Element-wise vector addition (in place).
   * Since breeze allocate new memory for this operation, this is actually not in-place.
   * @param vector operand vector
   * @return operation result
   */
  @Override
  public Vector addi(final Vector vector) {
    throw new UnsupportedOperationException();
  }

  /**
   * Element-wise vector addition.
   * The result is {@link DenseVector} if the operand is {@link DenseVector},
   * {@link SparseVector} otherwise.
   * @param vector operand vector
   * @return new vector with operation result
   */
  @Override
  public Vector add(final Vector vector) {
    if (vector.isDense()) {
      return new DenseVector((breeze.linalg.DenseVector<Float>)
          ((DenseVector) vector).getBreezeVector().$plus(breezeVector, VectorOps.ADD_DS));
    } else {
      return new SparseVector((breeze.linalg.SparseVector<Float>)
          breezeVector.$plus(((SparseVector) vector).breezeVector, VectorOps.ADD_SS));
    }
  }

  /**
   * Subtracts a scalar from all elements of this vector (in place).
   * @param value operand scalar
   * @return operation result
   */
  @Override
  public Vector subi(final float value) {
    ((NumericOps)breezeVector).$minus$eq(value, VectorOps.SUBI_ST);
    return this;
  }

  /**
   * Subtracts a scalar from all elements of this vector.
   * @param value operand scalar
   * @return operation result
   */
  @Override
  public Vector sub(final float value) {
    return new SparseVector((breeze.linalg.SparseVector<Float>) breezeVector.$minus(value, VectorOps.SUB_ST));
  }

  /**
   * Element-wise vector subtraction (in place).
   * Since breeze allocate new memory for this operation, this is actually not in-place.
   * @param vector operand vector
   * @return operation result
   */
  @Override
  public Vector subi(final Vector vector) {
    throw new UnsupportedOperationException();
  }

  /**
   * Element-wise vector subtraction.
   * The result is {@link DenseVector} if the operand is {@link DenseVector},
   * {@link SparseVector} otherwise.
   * @param vector operand vector
   * @return new vector with operation result
   */
  @Override
  public Vector sub(final Vector vector) {
    if (vector.isDense()) {
      return new DenseVector((breeze.linalg.DenseVector<Float>)
          ((DenseVector) vector).getBreezeVector().$minus(breezeVector, VectorOps.SUB_DS));
    } else {
      return new SparseVector((breeze.linalg.SparseVector<Float>)
          breezeVector.$minus(((SparseVector) vector).breezeVector, VectorOps.SUB_SS));
    }
  }

  /**
   * Multiplies a scala to all elements (in place).
   * Since breeze allocate new memory for this operation, this is actually not in-place.
   * @param value operand scala
   * @return operation result
   */
  @Override
  public Vector scalei(final float value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Multiplies a scala to all elements (in place).
   * Since breeze allocate new memory for this operation, this is actually not in-place.
   * @param value operand scala
   * @return new {@link SparseVector} with operation result
   */
  @Override
  public Vector scale(final float value) {
    return new SparseVector((breeze.linalg.SparseVector<Float>) breezeVector.$colon$times(value, VectorOps.SCALE_S));
  }

  /**
   * Divides all elements by a scalar (in place).
   * @param value operand scala
   * @return operation result
   */
  @Override
  public Vector divi(final float value) {
    ((NumericOps)breezeVector).$div$eq(value, VectorOps.DIVI_S);
    return this;
  }

  /**
   * Divides all elements by a scalar.
   * @param value operand scala
   * @return operation result
   */
  @Override
  public Vector div(final float value) {
    return new SparseVector((breeze.linalg.SparseVector<Float>) breezeVector.$div(value, VectorOps.DIV_S));
  }

  /**
   * In place axpy (y += a * x) operation.
   * @param value operand a
   * @param vector operand x
   * @return this vector with operation result
   */
  @Override
  public Vector axpy(final float value, final Vector vector) {
    if (vector.isDense()) {
      throw new UnsupportedOperationException();
    } else {
      package$.MODULE$.axpy(value, ((SparseVector) vector).breezeVector, breezeVector, VectorOps.AXPY_SS);
    }
    return this;
  }

  /**
   * Computes inner product.
   * @param vector operand vector
   * @return operation result
   */
  @Override
  public float dot(final Vector vector) {
    if (vector.isDense()) {
      return (float) ((DenseVector) vector).getBreezeVector().dot(breezeVector, VectorOps.DOT_DS);
    } else {
      return (float) breezeVector.dot(((SparseVector) vector).breezeVector, VectorOps.DOT_SS);
    }
  }

  private class SparseVectorIterator implements Iterator<VectorEntry> {

    private final Iterator<Tuple2<Object, Float>> iterator
        = JavaConversions.asJavaIterator(breezeVector.activeIterator());
    private final SparseVectorEntry entry = new SparseVectorEntry();

    public boolean hasNext() {
      return iterator.hasNext();
    }

    public VectorEntry next() {
      entry.cursor = iterator.next();
      return entry;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private class SparseVectorEntry implements VectorEntry {

    private Tuple2<Object, Float> cursor;

    public int index() {
      return (int) cursor._1();
    }

    public float value() {
      return cursor._2();
    }
  }
}
