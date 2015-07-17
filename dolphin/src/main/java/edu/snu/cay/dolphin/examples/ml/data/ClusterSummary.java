/**
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

import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;

import java.util.Formatter;
import java.util.Locale;

/**
 * This class represents a summary of the cluster
 * The summary includes (1) prior probability, (2) the centroid, and (3) the covariance matrix
 */
public final class ClusterSummary {
  private final double prior;
  private final Vector centroid;
  private final Matrix covariance;

  public ClusterSummary(double prior, Vector centroid, Matrix covariance) {
    this.prior = prior;
    this.centroid = centroid;
    this.covariance = covariance;
  }

  public final double getPrior() {
    return prior;
  }

  public final Vector getCentroid() {
    return centroid;
  }

  public final Matrix getCovariance() {
    return covariance;
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder("Cluster Summary:\n");
    try (final Formatter formatter = new Formatter(b, Locale.US)) {
      formatter.format("Prior probability: %f\n, ", prior);
      formatter.format("Centroid: ");
      for (int i = 0; i < centroid.size(); ++i) {
        formatter.format("%1.3f, ", centroid.get(i));
      }
      formatter.format("\n");
      formatter.format("Covariance:, ");
      for (int i = 0; i < covariance.rowSize(); ++i) {
        for (int j = 0; j < covariance.columnSize(); ++j) {
          formatter.format("%1.3f, ", covariance.get(i, j));
        }
        formatter.format("\n");
      }
      formatter.format("\n");
    }
    return b.toString();
  }
}
