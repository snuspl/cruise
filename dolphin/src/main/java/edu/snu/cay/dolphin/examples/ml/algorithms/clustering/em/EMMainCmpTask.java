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

import edu.snu.cay.dolphin.core.DataParser;
import edu.snu.cay.dolphin.core.ParseException;
import edu.snu.cay.dolphin.examples.ml.data.ClusterStats;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataBroadcastReceiver;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataReduceSender;
import edu.snu.cay.dolphin.core.UserComputeTask;
import edu.snu.cay.dolphin.examples.ml.data.ClusterSummary;
import edu.snu.cay.dolphin.examples.ml.parameters.IsCovarianceShared;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.exceptions.IdGenerationException;
import org.apache.mahout.math.*;
import org.apache.mahout.math.Vector;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;

public final class EMMainCmpTask extends UserComputeTask
    implements DataBroadcastReceiver<List<ClusterSummary>>, DataReduceSender<Map<Integer, ClusterStats>> {

  /**
   * Key used in Elastic Memory to put/get the data.
   */
  private static final String KEY_POINTS = "points";

  /**
   * Summaries of each cluster.
   */
  private List<ClusterSummary> clusterSummaries = new ArrayList<>();

  /**
   * Partial statistics of eah cluster.
   */
  private Map<Integer, ClusterStats> clusterToStats = new HashMap<>();

  /**
   * whether covariance matrices are diagonal or not.
   */
  private final boolean isCovarianceDiagonal;

  private final DataParser<List<Vector>> dataParser;

  /**
   * Memory storage to put/get the data.
   */
  private final MemoryStore memoryStore;

  private final DataIdFactory<Long> dataIdFactory;

  /**
   * Constructs a single Compute Task for the EM algorithm.
   * This class is instantiated by TANG.
   * @param dataParser
   * @param memoryStore Memory storage to put/get the data
   * @param isCovarianceDiagonal  whether covariance matrices are diagonal or not
   */
  @Inject
  public EMMainCmpTask(
      final DataParser<List<Vector>> dataParser,
      final MemoryStore memoryStore,
      final DataIdFactory<Long> dataIdFactory,
      @Parameter(IsCovarianceShared.class) final boolean isCovarianceDiagonal) {
    this.dataParser = dataParser;
    this.memoryStore = memoryStore;
    this.dataIdFactory = dataIdFactory;
    this.isCovarianceDiagonal = isCovarianceDiagonal;
  }

  @Override
  public void initialize() throws ParseException, RuntimeException {
    // Points read from input data to work on
    final List<Vector> points = dataParser.get();
    try {
      final List<Long> ids = dataIdFactory.getIds(points.size());
      memoryStore.getElasticStore().putList(KEY_POINTS, ids, points);
    } catch (IdGenerationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run(final int iteration) {
    clusterToStats = new HashMap<>();
    final int numClusters = clusterSummaries.size();

    // Compute the partial statistics of each cluster
    final Map<?, Vector> points = memoryStore.getElasticStore().getAll(KEY_POINTS);
    for (final Vector vector : points.values()) {
      final int dimension = vector.size();
      Matrix outProd = null;

      if (isCovarianceDiagonal) {
        outProd = new SparseMatrix(dimension, dimension);
        for (int j = 0; j < dimension; j++) {
          outProd.set(j, j, vector.get(j) * vector.get(j));
        }
      } else {
        outProd = vector.cross(vector);
      }

      double denominator = 0;
      final double[] numerators = new double[numClusters];
      for (int i = 0; i < numClusters; i++) {
        final ClusterSummary clusterSummary = clusterSummaries.get(i);
        final Vector centroid = clusterSummary.getCentroid();
        final Matrix covariance = clusterSummary.getCovariance();
        final Double prior = clusterSummary.getPrior();

        final Vector differ = vector.minus(centroid);
        numerators[i] = prior / Math.sqrt(covariance.determinant())
            * Math.exp(differ.dot(inverse(covariance).times(differ)) / (-2));
        denominator += numerators[i];
      }

      for (int i = 0; i < numClusters; i++) {
        final double posterior = denominator == 0 ? 1.0 / numerators.length : numerators[i] / denominator;
        if (!clusterToStats.containsKey(i)) {
          clusterToStats.put(i, new ClusterStats(times(outProd, posterior),
              vector.times(posterior), posterior, false));
        } else {
          clusterToStats.get(i).add(new ClusterStats(times(outProd, posterior),
              vector.times(posterior), posterior, false));
        }
      }
    }
  }

  @Override
  public Map<Integer, ClusterStats> sendReduceData(final int iteration) {
    return clusterToStats;
  }

  @Override
  public void receiveBroadcastData(final int iteration, final List<ClusterSummary> data) {
    this.clusterSummaries = data;
  }

  /**
   * Compute the inverse of a given matrix.
   */
  private Matrix inverse(final Matrix matrix) {
    final int dimension = matrix.rowSize();
    final QRDecomposition qr = new QRDecomposition(matrix);
    return qr.solve(DiagonalMatrix.identity(dimension));
  }

  /**
   * Return a new matrix containing the product of each value of the recipient and the argument.
   * This method exploits sparsity of the matrix, that is, considers only non-zero entries.
   */
  private Matrix times(final Matrix matrix, final double scala) {
    final Matrix result = matrix.clone();
    final Iterator<MatrixSlice> sliceIterator = matrix.iterator();
    while (sliceIterator.hasNext()) {
      final MatrixSlice slice = sliceIterator.next();
      final int row = slice.index();
      for (final Vector.Element e : slice.nonZeroes()) {
        final int col = e.index();
        result.set(row, col, e.get() * scala);
      }
    }
    return result;
  }
}
