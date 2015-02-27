package edu.snu.reef.flexion.examples.ml.algorithms.kmeans;

import edu.snu.reef.flexion.core.UserControllerTask;
import edu.snu.reef.flexion.examples.ml.converge.ConvergenceCondition;
import edu.snu.reef.flexion.examples.ml.data.Centroid;
import edu.snu.reef.flexion.examples.ml.data.VectorSum;
import edu.snu.reef.flexion.examples.ml.parameters.MaxIterations;
import edu.snu.reef.flexion.examples.ml.sub.CentroidListCodec;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataBroadcastSender;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataGatherReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataReduceReceiver;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KMeansCtrlTask extends UserControllerTask
        implements IDataReduceReceiver<Map<Integer, VectorSum>>, IDataBroadcastSender<List<Centroid>>, IDataGatherReceiver<List<Vector>> {

    private static final Logger LOG = Logger.getLogger(KMeansCtrlTask.class.getName());

    /**
     * Check function to determine whether algorithm has converged or not.
     * This is separate from the default stop condition,
     * which is based on the number of iterations made.
     */
    private final ConvergenceCondition convergenceCondition;

    /**
     * Maximum number of iterations allowed before job stops
     */
    private final int maxIterations;

    /**
     * Vector sum of the points assigned to each cluster
     */
    private Map<Integer, VectorSum> pointSum = new HashMap<Integer, VectorSum>();

    /**
     * List of cluster centroids to distribute to Compute Tasks
     * Will be updated for each iteration
     */
    private final List<Centroid> centroids = new ArrayList<Centroid>();

    /**
     * Initial centroids passed from Compute Tasks
     */
    private List<Vector> initialCentroids = null;

    /**
     * This class is instantiated by TANG
     *
     * Constructs the Controller Task for k-means
     *
     * @param convergenceCondition conditions for checking convergence of algorithm
     * @param maxIterations maximum number of iterations allowed before job stops
     */
    @Inject
    public KMeansCtrlTask(final ConvergenceCondition convergenceCondition,
                          @Parameter(MaxIterations.class) final int maxIterations) {

        this.convergenceCondition = convergenceCondition;
        this.maxIterations = maxIterations;
    }

    @Override
    public void run(int iteration) {

        //First iteration
        if (iteration==0) {
            //do nothing
        }
        //Second iteration
        else if (iteration==1) {

            int clusterID = 0;
            for (final Vector vector : initialCentroids) {
                centroids.add(new Centroid(clusterID++, vector));
            }
        }
        else {
            for (final Integer id : pointSum.keySet()) {
                final VectorSum vectorSum = pointSum.get(id);
                final Centroid newCentroid = new Centroid(id, vectorSum.computeVectorMean());
                centroids.set(id, newCentroid);
            }

            LOG.log(Level.INFO, "********* Centroids after {0} iterations*********", iteration + 1);
            LOG.log(Level.INFO, "" + centroids);
            LOG.log(Level.INFO, "********* Centroids after {0} iterations*********", iteration + 1);
        }
    }

    @Override
    public boolean isTerminated(int iteration) {

        if (iteration==0 || iteration==1) {
            return false;
        }
        else {
            return convergenceCondition.checkConvergence(centroids)
                || (iteration >= maxIterations+2); // First two iterations are used for initialization
        }
    }

    @Override
    public List<Centroid> sendBroadcastData(int iteration) {
        return centroids;
    }

    @Override
    public void receiveReduceData(Map<Integer, VectorSum> pointSum) {
        this.pointSum = pointSum;
    }

    @Override
    public void receiveGatherData(List<List<Vector>> initialCentroids) {
        if (this.initialCentroids == null) {
            this.initialCentroids = new LinkedList<Vector>();
            // Flatten the given list of lists
            for(List<Vector> list : initialCentroids) {
                for(Vector vector: list){
                    this.initialCentroids.add(vector);
                }
            }
        }
    }

    @Override
    public Class<? extends Codec> getBroadcastCodecClass() {
        return CentroidListCodec.class;
    }

}
