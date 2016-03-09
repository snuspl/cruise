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
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.util.Iterator;

/**
 * Vector implementation based on breeze dense vector.
 * This class should be initialized by {@link edu.snu.cay.common.math.linalg.VectorFactory}.
 */
public class DenseVector implements Vector {

  private final breeze.linalg.DenseVector<Double> breezeVector;

  DenseVector(final breeze.linalg.DenseVector<Double> breezeVector) {
    this.breezeVector = breezeVector;
  }

  breeze.linalg.DenseVector<Double> getBreezeVector() {
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
   * Return the number of active elements, which is same as {@code length()}.
   * @return number of active elements
   */
  @Override
  public int activeSize() {
    return breezeVector.activeSize();
  }

  /**
   * Returns true if the vector is dense, false if sparse.
   * @return true
   */
  @Override
  public boolean isDense() {
    return true;
  }

  /**
   * Gets an element specified by given index.
   * @param index an index in range [0, length)
   * @return element specified by given index
   */
  @Override
  public double get(final int index) {
    return breezeVector.apply(index);
  }

  /**
   * Sets an element to given value.
   * @param index an index in range [0, length)
   * @param value value to be set
   */
  @Override
  public void set(final int index, final double value) {
    breezeVector.update(index, value);
  }

  @Override
  public boolean equals(final Object o) {
    if (o instanceof DenseVector) {
      return breezeVector.equals(((DenseVector) o).breezeVector);
    }
    if (o instanceof SparseVector) {
      return breezeVector.equals(((SparseVector) o).getBreezeVector());
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
  public DenseVector copy() {
    return new DenseVector(breezeVector.copy());
  }

  @Override
  public Iterator<VectorEntry> iterator() {
    return new DenseVectorIterator();
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
  public Vector addi(final double value) {
    ((NumericOps)breezeVector).$plus$eq(value, VectorOps.ADDI_DT);
    return this;
  }

  /**
   * Adds a scalar to all elements of this vector.
   * @param value operand scalar
   * @return operation result
   */
  @Override
  public Vector add(final double value) {
    return new DenseVector((breeze.linalg.DenseVector<Double>) breezeVector.$plus(value, VectorOps.ADD_DT));
  }

  /**
   * Element-wise vector addition (in place).
   * @param vector operand vector
   * @return this vector with operation result
   */
  @Override
  public Vector addi(final Vector vector) {
    if (vector.isDense()) {
      ((NumericOps)breezeVector).$colon$plus$eq(((DenseVector) vector).breezeVector, VectorOps.ADDI_DD);
    } else {
      ((NumericOps)breezeVector).$colon$plus$eq(((SparseVector) vector).getBreezeVector(), VectorOps.ADDI_DS);
    }
    return this;
  }

  /**
   * Element-wise vector addition.
   * @param vector operand vector
   * @return new {@link DenseVector} with operation result
   */
  @Override
  public Vector add(final Vector vector) {
    if (vector.isDense()) {
      return new DenseVector((breeze.linalg.DenseVector<Double>)
          breezeVector.$plus(((DenseVector) vector).breezeVector, VectorOps.ADD_DD));
    } else {
      return new DenseVector((breeze.linalg.DenseVector<Double>)
          breezeVector.$plus(((SparseVector) vector).getBreezeVector(), VectorOps.ADD_DS));
    }
  }

  /**
   * Subtracts a scalar from all elements of this vector (in place).
   * @param value operand scalar
   * @return operation result
   */
  @Override
  public Vector subi(final double value) {
    ((NumericOps)breezeVector).$minus$eq(value, VectorOps.SUBI_DT);
    return this;
  }

  /**
   * Subtracts a scalar from all elements of this vector.
   * @param value operand scalar
   * @return operation result
   */
  @Override
  public Vector sub(final double value) {
    return new DenseVector((breeze.linalg.DenseVector<Double>) breezeVector.$minus(value, VectorOps.SUB_DT));
  }

  /**
   * Element-wise vector subtraction (in place).
   * @param vector operand vector
   * @return this vector with operation result
   */
  @Override
  public Vector subi(final Vector vector) {
    if (vector.isDense()) {
      ((NumericOps)breezeVector).$colon$minus$eq(((DenseVector) vector).breezeVector, VectorOps.SUBI_DD);
    } else {
      ((NumericOps)breezeVector).$colon$minus$eq(((SparseVector) vector).getBreezeVector(), VectorOps.SUBI_DS);
    }
    return this;
  }

  /**
   * Element-wise vector subtraction.
   * @param vector operand vector
   * @return new {@link DenseVector} with operation result
   */
  @Override
  public Vector sub(final Vector vector) {
    if (vector.isDense()) {
      return new DenseVector((breeze.linalg.DenseVector<Double>)
          breezeVector.$minus(((DenseVector) vector).breezeVector, VectorOps.SUB_DD));
    } else {
      return new DenseVector((breeze.linalg.DenseVector<Double>)
          breezeVector.$minus(((SparseVector) vector).getBreezeVector(), VectorOps.SUB_DS));
    }
  }

  /**
   * Multiplies a scala to all elements (in place).
   * @param value operand scala
   * @return this vector with operation result
   */
  @Override
  public Vector scalei(final double value) {
    ((NumericOps)breezeVector).$colon$times$eq(value, VectorOps.SCALEI_D);
    return this;
  }

  /**
   * Multiplies a scala to all elements.
   * @param value operand scala
   * @return new {@link DenseVector} with operation result
   */
  @Override
  public Vector scale(final double value) {
    return new DenseVector((breeze.linalg.DenseVector<Double>) breezeVector.$colon$times(value, VectorOps.SCALE_D));
  }

  /**
   * Divides all elements by a scalar (in place).
   * @param value operand scala
   * @return operation result
   */
  @Override
  public Vector divi(final double value) {
    ((NumericOps)breezeVector).$div$eq(value, VectorOps.DIVI_D);
    return this;
  }

  /**
   * Divides all elements by a scalar.
   * @param value operand scala
   * @return operation result
   */
  @Override
  public Vector div(final double value) {
    return new DenseVector((breeze.linalg.DenseVector<Double>) breezeVector.$div(value, VectorOps.DIV_D));
  }

  /**
   * In place axpy (y += a * x) operation.
   * @param value operand a
   * @param vector operand x
   * @return this vector with operation result
   */
  @Override
  public Vector axpy(final double value, final Vector vector) {
    if (vector.isDense()) {
      package$.MODULE$.axpy(value, ((DenseVector) vector).breezeVector, breezeVector, VectorOps.AXPY_DD);
    } else {
      package$.MODULE$.axpy(value, ((SparseVector) vector).getBreezeVector(), breezeVector, VectorOps.AXPY_DS);
    }
    return this;
  }

  /**
   * Computes inner product.
   * @param vector operand vector
   * @return operation result
   */
  @Override
  public double dot(final Vector vector) {
    if (vector.isDense()) {
      return (double) breezeVector.dot(((DenseVector) vector).breezeVector, VectorOps.DOT_DD);
    } else {
      return (double) breezeVector.dot(((SparseVector) vector).getBreezeVector(), VectorOps.DOT_DS);
    }
  }

  private class DenseVectorIterator implements Iterator<VectorEntry> {

    private final Iterator<Tuple2<Object, Double>> iterator
        = JavaConversions.asJavaIterator(breezeVector.activeIterator());
    private final DenseVectorEntry entry = new DenseVectorEntry();

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

  private class DenseVectorEntry implements VectorEntry {

    private Tuple2<Object, Double> cursor;

    public int index() {
      return (int) cursor._1();
    }

    public double value() {
      return cursor._2();
    }
  }
}
