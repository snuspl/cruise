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
package edu.snu.cay.dolphin.breeze;

import breeze.linalg.package$;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.util.Iterator;

/**
 * Vector implementation based on breeze sparse vector.
 */
public class SparseVector implements Vector {

  private final breeze.linalg.SparseVector<Double> breezeVector;

  SparseVector(final breeze.linalg.SparseVector<Double> breezeVector) {
    this.breezeVector = breezeVector;
  }

  breeze.linalg.SparseVector<Double> getBreezeVector() {
    return breezeVector;
  }

  @Override
  public int length() {
    return breezeVector.length();
  }

  @Override
  public int activeSize() {
    return breezeVector.activeSize();
  }

  @Override
  public boolean isDense() {
    return true;
  }

  @Override
  public double get(final int index) {
    return breezeVector.array().apply(index);
  }

  @Override
  public void set(final int index, final double value) {
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

  @Override
  public Vector addi(final Vector vector) {
    // actually not inplace
    throw new UnsupportedOperationException();
  }

  @Override
  public Vector add(final Vector vector) {
    if (vector instanceof DenseVector) {
      return new DenseVector((breeze.linalg.DenseVector<Double>)
          ((DenseVector) vector).getBreezeVector().$plus(breezeVector, VectorOps.ADD_DS));
    } else {
      return new SparseVector((breeze.linalg.SparseVector<Double>)
          breezeVector.$plus(((SparseVector) vector).breezeVector, VectorOps.ADD_SS));
    }
  }

  @Override
  public Vector subi(final Vector vector) {
    // actually not inplace
    throw new UnsupportedOperationException();
  }

  @Override
  public Vector sub(final Vector vector) {
    if (vector instanceof DenseVector) {
      return new DenseVector((breeze.linalg.DenseVector<Double>)
          ((DenseVector) vector).getBreezeVector().$minus(breezeVector, VectorOps.SUB_DS));
    } else {
      return new SparseVector((breeze.linalg.SparseVector<Double>)
          breezeVector.$minus(((SparseVector) vector).breezeVector, VectorOps.SUB_SS));
    }
  }

  @Override
  public Vector scalei(final double value) {
    // actually not inplace
    throw new UnsupportedOperationException();
  }

  @Override
  public Vector scale(final double value) {
    return new SparseVector((breeze.linalg.SparseVector<Double>) breezeVector.$colon$times(value, VectorOps.SCALE_S));
  }

  @Override
  public Vector axpy(final double value, final Vector vector) {
    if (vector instanceof DenseVector) {
      throw new UnsupportedOperationException();
    } else {
      package$.MODULE$.axpy(value, ((SparseVector) vector).breezeVector, breezeVector, VectorOps.AXPY_SS);
    }
    return this;
  }

  @Override
  public double dot(final Vector vector) {
    if (vector instanceof DenseVector) {
      return (double) ((DenseVector) vector).getBreezeVector().dot(breezeVector, VectorOps.DOT_DS);
    } else {
      return (double) breezeVector.dot(((SparseVector) vector).breezeVector, VectorOps.DOT_SS);
    }
  }

  private class SparseVectorIterator implements Iterator<VectorEntry> {

    private final Iterator<Tuple2<Object, Double>> iterator
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

    private Tuple2<Object, Double> cursor;

    public int index() {
      return (int) cursor._1();
    }

    public double value() {
      return cursor._2();
    }
  }
}
