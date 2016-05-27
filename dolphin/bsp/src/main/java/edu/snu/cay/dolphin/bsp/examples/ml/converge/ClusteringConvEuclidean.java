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
package edu.snu.cay.dolphin.bsp.examples.ml.converge;

import edu.snu.cay.dolphin.bsp.examples.ml.data.EuclideanDistance;
import edu.snu.cay.dolphin.bsp.examples.ml.parameters.ConvergenceThreshold;
import org.apache.mahout.math.Vector;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;

/**
 * Default implementation of ClusteringConvCond.
 * Algorithm converges when every centroid has moved less than
 * a certain threshold after an iteration.
 */
public final class ClusteringConvEuclidean implements ClusteringConvCond {
  private ArrayList<Vector> oldCentroids;
  private final double convergenceThreshold;
  private final EuclideanDistance euclideanDistance;

  @Inject
  public ClusteringConvEuclidean(
      final EuclideanDistance euclideanDistance,
      @Parameter(ConvergenceThreshold.class) final double convergenceThreshold) {
    this.euclideanDistance = euclideanDistance;
    this.convergenceThreshold = convergenceThreshold;
  }

  @Override
  public boolean checkConvergence(final Iterable<Vector> centroids) {
    if (oldCentroids == null) {
      oldCentroids = new ArrayList<>();

      int clusterID = 0;
      for (final Vector centroid : centroids) {
        oldCentroids.add(clusterID++, centroid);
      }

      return false;
    }

    boolean hasConverged = true;
    int clusterID = 0;
    for (final Vector centroid : centroids) {
      final Vector oldCentroid = oldCentroids.get(clusterID);

      if (hasConverged
          && distance(centroid, oldCentroid) > convergenceThreshold) {
        hasConverged = false;
      }

      oldCentroids.add(clusterID, centroid);
      clusterID++;
    }

    return hasConverged;
  }

  public double distance(final Vector v1, final Vector v2) {
    return euclideanDistance.distance(v1, v2);
  }
}
