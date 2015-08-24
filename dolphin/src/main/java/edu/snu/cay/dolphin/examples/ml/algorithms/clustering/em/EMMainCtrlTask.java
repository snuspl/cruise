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
package edu.snu.cay.dolphin.examples.ml.algorithms.clustering.em;

import edu.snu.cay.dolphin.core.KeyValueStore;
import edu.snu.cay.dolphin.examples.ml.key.Centroids;
import edu.snu.cay.dolphin.examples.ml.parameters.MaxIterations;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataBroadcastSender;
import edu.snu.cay.dolphin.core.UserControllerTask;
import edu.snu.cay.dolphin.examples.ml.converge.ClusteringConvCond;
import edu.snu.cay.dolphin.examples.ml.data.ClusterStats;
import edu.snu.cay.dolphin.examples.ml.data.ClusterSummary;
import edu.snu.cay.dolphin.examples.ml.parameters.IsCovarianceShared;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataReduceReceiver;
import org.apache.mahout.math.DiagonalMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.data.output.OutputStreamProvider;
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

public final class EMMainCtrlTask extends UserControllerTask
    implements DataReduceReceiver<Map<Integer, ClusterStats>>, DataBroadcastSender<List<ClusterSummary>> {

  private static final Logger LOG = Logger.getLogger(EMMainCtrlTask.class.getName());

  /**
   * Check function to determine whether algorithm has converged or not.
   * This is separate from the default stop condition,
   * which is based on the number of iterations made.
   */
  private final ClusteringConvCond clusteringConvergenceCondition;

  /**
   * Maximum number of iterations allowed before job stops.
   */
  private final int maxIterations;

  /**
   * Aggregated statistics of each cluster received from Compute Task.
   */
  private Map<Integer, ClusterStats> clusterStatsMap = new HashMap<>();

  /**
   * List of the centroids of the clusters passed from the preprocess stage.
   * Will be updated for each iteration.
   */
  private List<Vector> centroids = new ArrayList<>();

  /**
   * List of the summaries of the clusters to distribute to Compute Tasks.
   * Will be updated for each iteration.
   */
  private List<ClusterSummary> clusterSummaries = new ArrayList<>();

  /**
   * Whether to share a covariance matrix among clusters or not.
   */
  private final boolean isCovarianceShared;
  private final KeyValueStore keyValueStore;
  private final OutputStreamProvider outputStreamProvider;

  /**
   * Constructs the Controller Task for EM.
   * This class is instantiated by TANG.
   *
   * @param clusteringConvergenceCondition  conditions for checking convergence of algorithm
   * @param keyValueStore
   * @param outputStreamProvider
   * @param maxIterations maximum number of iterations allowed before job stops
   * @param isCovarianceShared    whether clusters share one covariance matrix or not
   */
  @Inject
  public EMMainCtrlTask(final ClusteringConvCond clusteringConvergenceCondition,
                        final KeyValueStore keyValueStore,
                        final OutputStreamProvider outputStreamProvider,
                        @Parameter(MaxIterations.class) final int maxIterations,
                        @Parameter(IsCovarianceShared.class) final boolean isCovarianceShared) {

    this.clusteringConvergenceCondition = clusteringConvergenceCondition;
    this.keyValueStore = keyValueStore;
    this.outputStreamProvider = outputStreamProvider;
    this.maxIterations = maxIterations;
    this.isCovarianceShared = isCovarianceShared;
  }

  /**
   * Receive initial centroids from the preprocess task.
   */
  @Override
  public void initialize() {

    // Load the initial centroids from the previous stage
    centroids = keyValueStore.get(Centroids.class);

    // Initialize cluster summaries
    final int numClusters = centroids.size();
    for (int clusterID = 0; clusterID < numClusters; clusterID++) {
      final Vector vector = centroids.get(clusterID);
      final int dimension = vector.size();
      clusterSummaries.add(new ClusterSummary(1.0, vector,
          DiagonalMatrix.identity(dimension)));
    }
  }

  @Override
  public void run(final int iteration) {

    // Compute the shared covariance matrix if necessary
    Matrix covarianceMatrix = null;
    if (isCovarianceShared) {
      ClusterStats clusterStatsSum = null;
      for (final Integer id : clusterStatsMap.keySet()) {
        final ClusterStats clusterStats = clusterStatsMap.get(id);
        if (clusterStatsSum == null) {
          clusterStatsSum = new ClusterStats(clusterStats, true);
        } else {
          clusterStatsSum.add(clusterStats);
        }
      }
      if (clusterStatsSum != null) {
        covarianceMatrix = clusterStatsSum.computeCovariance();
      }
    }

    // Compute new prior probability, centroids, and covariance matrices
    for (final Integer clusterID : clusterStatsMap.keySet()) {
      final ClusterStats clusterStats = clusterStatsMap.get(clusterID);
      final Vector newCentroid = clusterStats.computeMean();
      Matrix newCovariance = null;
      if (isCovarianceShared) {
        newCovariance = covarianceMatrix;
      } else {
        newCovariance = clusterStats.computeCovariance();
      }
      final double newPrior = clusterStats.getProbSum(); //unnormalized prior

      centroids.set(clusterID, newCentroid);
      clusterSummaries.set(clusterID, new ClusterSummary(newPrior, newCentroid, newCovariance));
    }

    LOG.log(Level.INFO, "********* Centroids after {0} iterations*********", iteration + 1);
    LOG.log(Level.INFO, "" + clusterSummaries);
    LOG.log(Level.INFO, "********* Centroids after {0} iterations*********", iteration + 1);
  }

  @Override
  public boolean isTerminated(final int iteration) {
    return clusteringConvergenceCondition.checkConvergence(centroids)
        || (iteration >= maxIterations);
  }

  @Override
  public void receiveReduceData(final int iteration, final Map<Integer, ClusterStats> data) {
    clusterStatsMap = data;
  }

  @Override
  public List<ClusterSummary> sendBroadcastData(final int iteration) {
    return clusterSummaries;
  }

  @Override
  public void cleanup() {

    //output the centroids and covariances of the clusters
    try (final DataOutputStream centroidStream = outputStreamProvider.create("centroids");
         final DataOutputStream covarianceStream = outputStreamProvider.create("covariances")
    ) {
      centroidStream.writeBytes(String.format("cluster_id,centroid%n"));
      for (int i = 0; i < centroids.size(); i++) {
        centroidStream.writeBytes(String.format("%d,%s%n", (i + 1), centroids.get(i).toString()));
      }
      covarianceStream.writeBytes(String.format("cluster_id,covariance%n"));
      for (int i = 0; i < centroids.size(); i++) {
        covarianceStream.writeBytes(
            String.format("%d,%s%n", (i + 1), clusterSummaries.get(i).getCovariance().toString()));
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
