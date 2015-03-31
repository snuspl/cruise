/**
 * Copyright (C) 2014 Seoul National University
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
package edu.snu.reef.flexion.examples.ml.data;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

import java.io.Serializable;
import java.util.Formatter;
import java.util.Locale;

/**
 * This class represents a set of vectors by their sum and count.
 */
public final class VectorSum implements Serializable {

  public Vector sum;
  public int count;

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
   * A deep copy constructor
   */
  public VectorSum(final VectorSum vectorSum) {
    this(vectorSum.sum, vectorSum.count, true);
  }

  public final void add(VectorSum vectorSum) {
    this.sum = this.sum.plus(vectorSum.sum);
    this.count += vectorSum.count;
  }

  public final void add(Vector vector) {
    this.sum = this.sum.plus(vector);
    this.count++;
  }

  public final void addSums(Iterable<VectorSum> vectorSums) {
    for (final VectorSum vectorSum : vectorSums) {
      this.add(vectorSum);
    }
  }

  public final void addVectors(Iterable<Vector> vectors) {
    for (final Vector vector : vectors) {
      this.add(vector);
    }
  }

  public final static VectorSum addAllSums(Iterable<VectorSum> vectorSums) {
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

  public final static VectorSum addAllVectors(Iterable<Vector> vectors) {
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

  public final Vector computeVectorMean() {
    final Vector mean = new DenseVector(sum.size());
    for (int i = 0; i < mean.size(); i++) {
      mean.set(i, sum.get(i) / count);
    }
    return mean;
  }

  @SuppressWarnings("boxing")
  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder("VectorSum(");
    try (final Formatter formatter = new Formatter(b, Locale.US)) {
      formatter.format("Count %d, ", count);

      if (sum == null || sum.size() == 0) {
        formatter.format("Empty)");
      } else {
        for (int i = 0; i < sum.size() - 1; ++i) {
          formatter.format("%1.3f, ", sum.get(i));
        }
        formatter.format("%1.3f)", sum.get(sum.size() - 1));
      }
    }

    return b.toString();
  }
}
