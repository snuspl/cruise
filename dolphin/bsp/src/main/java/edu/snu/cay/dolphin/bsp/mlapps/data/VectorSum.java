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
package edu.snu.cay.dolphin.bsp.mlapps.data;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

import java.io.Serializable;
import java.util.Formatter;
import java.util.Locale;

/**
 * This class represents a set of vectors by their sum and count.
 */
public final class VectorSum implements Serializable {
  private Vector sum;
  private int count;

  /**
   * We may select whether to create a deep copy of @member sum, or just a reference.
   */
  public VectorSum(final Vector sum, final int count, final boolean isDeepCopy) {
    this.count = count;
    if (isDeepCopy) {
      final Vector newSum = new DenseVector(sum.size());
      for (int i = 0; i < newSum.size(); i++) {
        newSum.set(i, sum.get(i));
      }
      this.sum = newSum;
    } else {
      this.sum = sum;
    }
  }

  public VectorSum(final Vector sum, final int count) {
    this(sum, count, false);
  }

  /**
   * A deep copy constructor.
   */
  public VectorSum(final VectorSum vectorSum) {
    this(vectorSum.getSum(), vectorSum.getCount(), true);
  }

  public void add(final VectorSum vectorSum) {
    this.sum = this.getSum().plus(vectorSum.getSum());
    this.count += vectorSum.getCount();
  }

  public void add(final Vector vector) {
    this.sum = this.getSum().plus(vector);
    this.count += 1;
  }

  public void addSums(final Iterable<VectorSum> vectorSums) {
    for (final VectorSum vectorSum : vectorSums) {
      this.add(vectorSum);
    }
  }

  public void addVectors(final Iterable<Vector> vectors) {
    for (final Vector vector : vectors) {
      this.add(vector);
    }
  }

  public static VectorSum addAllSums(final Iterable<VectorSum> vectorSums) {
    VectorSum totalVectorSum = null;
    for (final VectorSum vectorSum : vectorSums) {
      if (totalVectorSum == null) {
        totalVectorSum = new VectorSum(vectorSum);
        continue;
      }
      totalVectorSum.add(vectorSum);
    }
    return totalVectorSum;
  }

  public static VectorSum addAllVectors(final Iterable<Vector> vectors) {
    VectorSum vectorSum = null;
    for (final Vector vector : vectors) {
      if (vectorSum == null) {
        vectorSum = new VectorSum(vector, 1, true);
        continue;
      }
      vectorSum.add(vector);
    }
    return vectorSum;
  }

  public Vector computeVectorMean() {
    final Vector mean = new DenseVector(getSum().size());
    for (int i = 0; i < mean.size(); i++) {
      mean.set(i, getSum().get(i) / getCount());
    }
    return mean;
  }

  @SuppressWarnings("boxing")
  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder("VectorSum(");
    try (Formatter formatter = new Formatter(b, Locale.US)) {
      formatter.format("Count %d, ", getCount());

      if (getSum() == null || getSum().size() == 0) {
        formatter.format("Empty)");
      } else {
        for (int i = 0; i < getSum().size() - 1; ++i) {
          formatter.format("%1.3f, ", getSum().get(i));
        }
        formatter.format("%1.3f)", getSum().get(getSum().size() - 1));
      }
    }
    return b.toString();
  }

  public Vector getSum() {
    return sum;
  }

  public int getCount() {
    return count;
  }
}
