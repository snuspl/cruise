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
package edu.snu.cay.dolphin.examples.ml.data;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.Vector;

import java.io.Serializable;
import java.util.Iterator;

/**
 * This class represents statistics obtained from data points assigned to each cluster.
 * Statistics include (1) the pointSum of probabilities (2) the weighted pointSum of data points,
 * and (3) the weighted pointSum of the outer product of data points.
 */
public final class ClusterStats implements Serializable {

  private Matrix outProdSum;
  private Vector pointSum;
  private double probSum = 0; // error occurs without initialize

  /**
   * We may select whether to create a deep copy of @member pointSum and @member outProdSum, or just a reference.
   * @param outProdSum
   * @param pointSum
   * @param probSum
   * @param isDeepCopy
   */
  public ClusterStats(final Matrix outProdSum, final Vector pointSum, final double probSum, final boolean isDeepCopy) {
    if (isDeepCopy) {
      this.outProdSum = outProdSum.clone();
      this.pointSum = pointSum.clone();
    } else {
      this.outProdSum = outProdSum;
      this.pointSum = pointSum;
    }
    this.probSum = probSum;
  }

  public ClusterStats(final Matrix outProdSum, final Vector pointSum, final double probSum) {
    this(outProdSum, pointSum, probSum, false);
  }

  /**
   * A deep copy constructor.
   */
  public ClusterStats(final ClusterStats clusterStats, final boolean isDeepCopy) {
    this(clusterStats.getOutProdSum(), clusterStats.getPointSum(), clusterStats.getProbSum(), isDeepCopy);
  }

  /**
   * Add the given statistics to the current statistics.
   * @param clusterStats
   */
  public void add(final ClusterStats clusterStats) {
    this.outProdSum = this.getOutProdSum().plus(clusterStats.getOutProdSum());
    this.pointSum = this.getPointSum().plus(clusterStats.getPointSum());
    this.probSum += clusterStats.getProbSum();
  }

  /**
   * Compute mean from the statistics.
   * @return
   */
  public Vector computeMean() {
    final Vector mean = new DenseVector(getPointSum().size());
    for (int i = 0; i < mean.size(); i++) {
      mean.set(i, getPointSum().get(i) / getProbSum());
    }
    return mean;
  }

  /**
   * Compute the covariance matrix from the statistics.
   * @return
   */
  public Matrix computeCovariance() {
    final Vector mean = computeMean();
    final Matrix covariance = getOutProdSum().clone();

    final Iterator<MatrixSlice> sliceIterator = getOutProdSum().iterator();
    while (sliceIterator.hasNext()) {
      final MatrixSlice slice = sliceIterator.next();
      final int row = slice.index();
      for (final Vector.Element e : slice.nonZeroes()) {
        final int col = e.index();
        final double squaredSum = e.get();
        covariance.set(row, col, squaredSum / getProbSum() - mean.get(row) * mean.get(col));
      }
    }
    return covariance;
  }

  /**
   * weighted sum of outer product of data points.
   */
  public Matrix getOutProdSum() {
    return outProdSum;
  }

  /**
   * weighted pointSum of data points.
   */
  public Vector getPointSum() {
    return pointSum;
  }

  /**
   * pointSum of probability.
   */
  public double getProbSum() {
    return probSum;
  }
}
