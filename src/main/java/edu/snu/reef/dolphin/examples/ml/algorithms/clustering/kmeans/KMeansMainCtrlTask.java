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
package edu.snu.reef.dolphin.examples.ml.algorithms.clustering.kmeans;

import edu.snu.reef.dolphin.core.KeyValueStore;
import edu.snu.reef.dolphin.core.OutputStreamProvider;
import edu.snu.reef.dolphin.core.UserControllerTask;
import edu.snu.reef.dolphin.examples.ml.converge.ClusteringConvCond;
import edu.snu.reef.dolphin.examples.ml.data.VectorSum;
import edu.snu.reef.dolphin.examples.ml.key.Centroids;
import edu.snu.reef.dolphin.examples.ml.parameters.MaxIterations;
import edu.snu.reef.dolphin.groupcomm.interfaces.DataBroadcastSender;
import edu.snu.reef.dolphin.groupcomm.interfaces.DataReduceReceiver;
import org.apache.mahout.math.Vector;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class KMeansMainCtrlTask extends UserControllerTask
    implements DataReduceReceiver<Map<Integer, VectorSum>>, DataBroadcastSender<List<Vector>> {

  private static final Logger LOG = Logger.getLogger(KMeansMainCtrlTask.class.getName());

  /**
   * Check function to determine whether algorithm has converged or not.
   * This is separate from the default stop condition,
   * which is based on the number of iterations made.
   */
  private final ClusteringConvCond clusteringConvergenceCondition;

  /**
   * Maximum number of iterations allowed before job stops
   */
  private final int maxIterations;

  /**
   * Vector sum of the points assigned to each cluster
   */
  private Map<Integer, VectorSum> pointSum = new HashMap<>();

  /**
   * List of cluster centroids to distribute to Compute Tasks
   * Will be updated for each iteration
   */
  private List<Vector> centroids = new ArrayList<>();
  private final KeyValueStore keyValueStore;
  private final OutputStreamProvider outputStreamProvider;

  /**
   * This class is instantiated by TANG
   *
   * Constructs the Controller Task for k-means
   *
   * @param clusteringConvergenceCondition conditions for checking convergence of algorithm
   * @param keyValueStore
   * @param outputStreamProvider
   * @param maxIterations maximum number of iterations allowed before job stops
   */
  @Inject
  public KMeansMainCtrlTask(final ClusteringConvCond clusteringConvergenceCondition,
                            final KeyValueStore keyValueStore,
                            final OutputStreamProvider outputStreamProvider,
                            @Parameter(MaxIterations.class) final int maxIterations) {

    this.clusteringConvergenceCondition = clusteringConvergenceCondition;
    this.outputStreamProvider = outputStreamProvider;
    this.keyValueStore = keyValueStore;
    this.maxIterations = maxIterations;
  }

  /**
   * Receive initial centroids from the preprocess task
   */
  @Override
  public void initialize() {
    centroids = keyValueStore.get(Centroids.class);

  }

  @Override
  public void run(int iteration) {
    for (final Integer clusterID : pointSum.keySet()) {
      final VectorSum vectorSum = pointSum.get(clusterID);
      final Vector newCentroid = vectorSum.computeVectorMean();
      centroids.set(clusterID, newCentroid);
    }

    LOG.log(Level.INFO, "********* Centroids after {0} iterations*********", iteration);
    LOG.log(Level.INFO, "" + centroids);
    LOG.log(Level.INFO, "********* Centroids after {0} iterations*********", iteration);
  }

  @Override
  public boolean isTerminated(int iteration) {
    return clusteringConvergenceCondition.checkConvergence(centroids)
        || (iteration >= maxIterations);
  }

  @Override
  public List<Vector> sendBroadcastData(int iteration) {
    return centroids;
  }

  @Override
  public void receiveReduceData(int iteration, Map<Integer, VectorSum> pointSum) {
    this.pointSum = pointSum;
  }

  @Override
  public void cleanup() {

    //output the centroids of the clusters
    DataOutputStream outputStream = null;
    try {
      outputStream = outputStreamProvider.create("centroids");
      outputStream.writeBytes("cluster_id,centroid\n");
      for(int i=0; i<centroids.size(); i++) {
        outputStream.writeBytes(String.format("%d,%s\n", (i + 1), centroids.get(i).toString()));
      }
    } catch (Exception e){
      e.printStackTrace();
    } finally {
      try { outputStream.close(); } catch (IOException e) {}
    }
  }

}
