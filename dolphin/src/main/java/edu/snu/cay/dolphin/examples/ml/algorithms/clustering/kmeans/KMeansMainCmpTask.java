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
package edu.snu.cay.dolphin.examples.ml.algorithms.clustering.kmeans;

import edu.snu.cay.dolphin.core.DataParser;
import edu.snu.cay.dolphin.core.ParseException;
import edu.snu.cay.dolphin.examples.ml.data.VectorSum;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataBroadcastReceiver;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataReduceSender;
import edu.snu.cay.dolphin.core.UserComputeTask;
import edu.snu.cay.dolphin.examples.ml.data.VectorDistanceMeasure;
import edu.snu.cay.em.evaluator.api.MemoryStore;
import org.apache.mahout.math.Vector;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class KMeansMainCmpTask extends UserComputeTask
    implements DataBroadcastReceiver<List<Vector>>, DataReduceSender<Map<Integer, VectorSum>> {

  /**
   * Key used in Elastic Memory to put/get the data
   */
  private static final String KEY_POINTS = "points";

  /**
   * Centroids of clusters
   */
  private List<Vector> centroids = new ArrayList<>();

  /**
   * Vector sum of the points assigned to each cluster
   */
  private Map<Integer, VectorSum> pointSum = new HashMap<>();

  /**
   * Definition of 'distance between points' for this job
   * Default measure is Euclidean distance
   */
  private final VectorDistanceMeasure distanceMeasure;

  private final DataParser<List<Vector>> dataParser;

  /**
   * Memory storage to put/get the data.
   */
  private final MemoryStore memoryStore;

  /**
   * This class is instantiated by TANG
   * Constructs a single Compute Task for k-means
   * @param dataParser
   * @param memoryStore Memory storage to put/get the data
   * @param distanceMeasure distance measure to use to compute distances between points
   */
  @Inject
  public KMeansMainCmpTask(final VectorDistanceMeasure distanceMeasure,
                           final DataParser<List<Vector>> dataParser,
                           final MemoryStore memoryStore) {
    this.distanceMeasure = distanceMeasure;
    this.dataParser = dataParser;
    this.memoryStore = memoryStore;
  }

  @Override
  public void initialize() throws ParseException {
    // Points read from input data to work on
    final List<Vector> points = dataParser.get();
    memoryStore.putMovable(KEY_POINTS, points);
  }

  @Override
  public void run(int iteration) {

    // Compute the nearest cluster centroid for each point
    pointSum = new HashMap<>();
    final List<Vector> points = memoryStore.get(KEY_POINTS);
    for (final Vector vector : points) {
      double nearestClusterDist = Double.MAX_VALUE;
      int nearestClusterId = -1;
      int clusterId = 0;
      for (Vector centroid : centroids) {
        final double distance = distanceMeasure.distance(centroid, vector);
        if (nearestClusterDist > distance) {
          nearestClusterDist = distance;
          nearestClusterId = clusterId;
        }
        clusterId++;
      }

      // Compute vector sums for each cluster centroid
      if (pointSum.containsKey(nearestClusterId)) {
        pointSum.get(nearestClusterId).add(vector);
      } else {
        pointSum.put(nearestClusterId, new VectorSum(vector, 1, true));
      }
    }
  }

  @Override
  public void receiveBroadcastData(int iteration, List<Vector> centroids) {
    this.centroids = centroids;
  }

  @Override
  public Map<Integer, VectorSum> sendReduceData(int iteration) {
    return pointSum;
  }
}