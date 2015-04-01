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
 * A cluster centroid implementation of k-means and EM clustering
 */
public final class Centroid implements Serializable {

  private final int clusterId;
  public final Vector vector;

  /**
   * This constructor does not create a deep copy of @param vector.
   */
  public Centroid(final int clusterId, final Vector vector) {
    this.clusterId = clusterId;
    this.vector = vector;
  }

  /**
   * A copy constructor that creates a deep copy of a centroid.
   *
   * The newly created KMeansCentroid does not reference
   * anything from the original KMeansCentroid.
   */
  public Centroid(final Centroid centroid) {
    this.clusterId = centroid.clusterId;
    final Vector vector = new DenseVector(centroid.vector.size());
    for (int i = 0; i < vector.size(); i++) {
      vector.set(i, centroid.vector.get(i));
    }
    this.vector = vector;
  }

  public final int dimension(){
      return vector.size();
  }

  public final int getClusterId() {
    return clusterId;
  }

  public final Vector getVector() { return vector; }

  @SuppressWarnings("boxing")
  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder("Centroid(");
    try (final Formatter formatter = new Formatter(b, Locale.US)) {
      formatter.format("Id %d, ", this.clusterId);
      for (int i = 0; i < this.vector.size() - 1; ++i) {
        formatter.format("%1.3f, ", this.vector.get(i));
      }
      formatter.format("%1.3f", this.vector.get(this.vector.size() - 1));
    }
    b.append(')');
    return b.toString();
  }

}
