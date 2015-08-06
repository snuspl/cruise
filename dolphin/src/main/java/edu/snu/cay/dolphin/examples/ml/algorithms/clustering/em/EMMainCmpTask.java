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
import org.apache.mahout.math.*;
import org.apache.mahout.math.Vector;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;

public final class EMMainCmpTask extends UserComputeTask
    implements DataBroadcastReceiver<List<ClusterSummary>>, DataReduceSender<Map<Integer, ClusterStats>> {

  /**
   * Points read from input data to work on.
   */
  private List<Vector> points = null;

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
   * Constructs a single Compute Task for the EM algorithm.
   * This class is instantiated by TANG.
   * @param dataParser
   * @param isCovarianceDiagonal  whether covariance matrices are diagonal or not
   */
  @Inject
  public EMMainCmpTask(
      final DataParser<List<Vector>> dataParser,
      @Parameter(IsCovarianceShared.class) final boolean isCovarianceDiagonal) {
    this.dataParser = dataParser;
    this.isCovarianceDiagonal = isCovarianceDiagonal;
  }

  @Override
  public void initialize() throws ParseException {
    points = dataParser.get();
  }

  @Override
  public void run(final int iteration) {
    clusterToStats = new HashMap<>();
    final int numClusters = clusterSummaries.size();

    // Compute the partial statistics of each cluster
    for (final Vector vector : points) {
      final int dimension = vector.size();
      Matrix outProd = null;

      if (isCovarianceDiagonal) {
        outProd = new SparseMatrix(dimension, dimension);
        for (int j=0; j<dimension; j++) {
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
            * Math.exp(differ.dot(inverse(covariance).times(differ))/(-2));
        denominator += numerators[i];
      }

      for (int i = 0; i < numClusters; i++) {
        final double posterior = denominator == 0 ? 1.0 / numerators.length : numerators[i]/denominator;
        if (!clusterToStats.containsKey(i)) {
          clusterToStats.put(i, new ClusterStats(times(outProd, posterior),
              vector.times(posterior), posterior, false));
        } else{
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
    final Iterator<MatrixSlice> sliceIterator=matrix.iterator();
    while (sliceIterator.hasNext()) {
      final MatrixSlice slice=sliceIterator.next();
      final int row = slice.index();
      for (final Vector.Element e : slice.nonZeroes()) {
        final int col=e.index();
        result.set(row, col, e.get() * scala);
      }
    }
    return result;
  }
}
