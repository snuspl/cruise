package edu.snu.reef.flexion.examples.ml.algorithms.em;

import edu.snu.reef.flexion.core.UserComputeTask;
import edu.snu.reef.flexion.examples.ml.data.ClusterStats;
import edu.snu.reef.flexion.examples.ml.data.ClusterSummary;
import edu.snu.reef.flexion.examples.ml.parameters.IsCovarianceShared;
import edu.snu.reef.flexion.groupcomm.interfaces.DataBroadcastReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.DataReduceSender;
import org.apache.mahout.math.*;
import org.apache.mahout.math.Vector;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;

public final class EMMainCmpTask extends UserComputeTask<List<Vector>>
        implements DataBroadcastReceiver<List<ClusterSummary>>, DataReduceSender<Map<Integer, ClusterStats>> {

    /**
     * Points read from input data to work on
     */
    private List<Vector> points = null;

    /**
     * Summaries of each cluster
     */
    private List<ClusterSummary> clusterSummaries = new ArrayList<>();

    /**
     * Partial statistics of eah cluster
     */
    private Map<Integer, ClusterStats> clusterToStats = new HashMap<>();

    /**
     * whether covariance matrices are diagonal or not
     */
    private final boolean isCovarianceDiagonal;

    /**
     * This class is instantiated by TANG
     * Constructs a single Compute Task for the EM algorithm
     * @param isCovarianceDiagonal  whether covariance matrices are diagonal or not
     */
    @Inject
    public EMMainCmpTask(@Parameter(IsCovarianceShared.class) final boolean isCovarianceDiagonal) {
        this.isCovarianceDiagonal = isCovarianceDiagonal;
    }

    @Override
    public void run(int iteration, List<Vector> data) {

        points = data;

        clusterToStats = new HashMap<>();
        int numClusters = clusterSummaries.size();

        // Compute the partial statistics of each cluster
        for (final Vector vector : points) {
            int dimension = vector.size();
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
            double[] numerators = new double[numClusters];
            for (int i = 0; i < numClusters; i++) {
                ClusterSummary clusterSummary = clusterSummaries.get(i);
                Vector centroid = clusterSummary.getCentroid().getVector();
                Matrix covariance = clusterSummary.getCovariance().getMatrix();
                Double prior = clusterSummary.getPrior();

                Vector differ = vector.minus(centroid);
                numerators[i] = prior / Math.sqrt(covariance.determinant())
                        * Math.exp(differ.dot(inverse(covariance).times(differ))/(-2));
                denominator += numerators[i];
            }

            for (int i = 0; i < numClusters; i++) {
                double posterior = denominator == 0 ? 1.0 / numerators.length : numerators[i]/denominator;
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
    public Map<Integer, ClusterStats> sendReduceData(int iteration) {
        return clusterToStats;
    }

    @Override
    public void receiveBroadcastData(List<ClusterSummary> data) {
        this.clusterSummaries = data;
    }

    /**
     * Compute the inverse of a given matrix
     */
    private final Matrix inverse(Matrix matrix) {
        int dimension = matrix.rowSize();
        QRDecomposition qr = new QRDecomposition(matrix);
        return qr.solve(DiagonalMatrix.identity(dimension));
    }


    /**
     * Return a new matrix containing the product of each value of the recipient and the argument
     * This method exploits sparsity of the matrix, that is, considers only non-zero entries
     */
    private final Matrix times (Matrix matrix, double scala) {
        final Matrix result = matrix.clone();

        Iterator<MatrixSlice> sliceIterator=matrix.iterator();
        while (sliceIterator.hasNext()) {
            MatrixSlice slice=sliceIterator.next();
            int row = slice.index();
            for (Vector.Element e : slice.nonZeroes()) {
                int col=e.index();
                result.set(row, col, e.get() * scala);
            }
        }
        return result;

    }
}