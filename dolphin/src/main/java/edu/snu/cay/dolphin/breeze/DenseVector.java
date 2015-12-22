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

import breeze.linalg.NumericOps;
import breeze.linalg.package$;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.util.Iterator;

/**
 * Vector implementation based on breeze dense vector.
 */
public class DenseVector implements Vector {

  private final breeze.linalg.DenseVector<Double> breezeVector;

  DenseVector(final breeze.linalg.DenseVector<Double> breezeVector) {
    this.breezeVector = breezeVector;
  }

  breeze.linalg.DenseVector<Double> getBreezeVector() {
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
    return breezeVector.unsafeValueAt(index);
  }

  @Override
  public void set(final int index, final double value) {
    breezeVector.unsafeUpdate(index, value);
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

  @Override
  public Vector addi(final Vector vector) {
    if (vector instanceof DenseVector) {
      ((NumericOps)breezeVector).$colon$plus$eq(((DenseVector) vector).breezeVector, VectorOps.ADDI_DD);
    } else {
      ((NumericOps)breezeVector).$colon$plus$eq(((SparseVector) vector).getBreezeVector(), VectorOps.ADDI_DS);
    }
    return this;
  }

  @Override
  public Vector add(final Vector vector) {
    if (vector instanceof DenseVector) {
      return new DenseVector((breeze.linalg.DenseVector<Double>)
          breezeVector.$plus(((DenseVector) vector).breezeVector, VectorOps.ADD_DD));
    } else {
      return new DenseVector((breeze.linalg.DenseVector<Double>)
          breezeVector.$plus(((SparseVector) vector).getBreezeVector(), VectorOps.ADD_DS));
    }
  }

  @Override
  public Vector subi(final Vector vector) {
    if (vector instanceof DenseVector) {
      ((NumericOps)breezeVector).$colon$minus$eq(((DenseVector) vector).breezeVector, VectorOps.SUBI_DD);
    } else {
      ((NumericOps)breezeVector).$colon$minus$eq(((SparseVector) vector).getBreezeVector(), VectorOps.SUBI_DS);
    }
    return this;
  }

  @Override
  public Vector sub(final Vector vector) {
    if (vector instanceof DenseVector) {
      return new DenseVector((breeze.linalg.DenseVector<Double>)
          breezeVector.$minus(((DenseVector) vector).breezeVector, VectorOps.SUB_DD));
    } else {
      return new DenseVector((breeze.linalg.DenseVector<Double>)
          breezeVector.$minus(((SparseVector) vector).getBreezeVector(), VectorOps.SUB_DS));
    }
  }

  @Override
  public Vector scalei(final double value) {
    ((NumericOps)breezeVector).$colon$times$eq(value, VectorOps.SCALEI_D);
    return this;
  }

  @Override
  public Vector scale(final double value) {
    return new DenseVector((breeze.linalg.DenseVector<Double>) breezeVector.$colon$times(value, VectorOps.SCALE_D));
  }

  @Override
  public Vector axpy(final double value, final Vector vector) {
    if (vector instanceof DenseVector) {
      package$.MODULE$.axpy(value, ((DenseVector) vector).breezeVector, breezeVector, VectorOps.AXPY_DD);
    } else {
      package$.MODULE$.axpy(value, ((SparseVector) vector).getBreezeVector(), breezeVector, VectorOps.AXPY_DS);
    }
    return this;
  }

  @Override
  public double dot(final Vector vector) {
    if (vector instanceof DenseVector) {
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
