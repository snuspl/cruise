package edu.snu.reef.flexion.examples.ml.algorithms.kmeans;

import edu.snu.reef.flexion.core.KeyValueStore;
import edu.snu.reef.flexion.core.UserControllerTask;
import edu.snu.reef.flexion.examples.ml.converge.ConvergenceCondition;
import edu.snu.reef.flexion.examples.ml.data.VectorSum;
import edu.snu.reef.flexion.examples.ml.key.Centroids;
import edu.snu.reef.flexion.examples.ml.parameters.MaxIterations;
import edu.snu.reef.flexion.groupcomm.interfaces.DataBroadcastSender;
import edu.snu.reef.flexion.groupcomm.interfaces.DataReduceReceiver;
import org.apache.mahout.math.Vector;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
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
    private final ConvergenceCondition convergenceCondition;

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

    /**
     * This class is instantiated by TANG
     *
     * Constructs the Controller Task for k-means
     *
     * @param convergenceCondition conditions for checking convergence of algorithm
     * @param keyValueStore
     * @param maxIterations maximum number of iterations allowed before job stops
     */
    @Inject
    public KMeansMainCtrlTask(final ConvergenceCondition convergenceCondition,
                              final KeyValueStore keyValueStore,
                              @Parameter(MaxIterations.class) final int maxIterations) {

        this.convergenceCondition = convergenceCondition;
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
        return convergenceCondition.checkConvergence(centroids)
            || (iteration > maxIterations);

    }

    @Override
    public List<Vector> sendBroadcastData(int iteration) {
        return centroids;
    }

    @Override
    public void receiveReduceData(Map<Integer, VectorSum> pointSum) {
        this.pointSum = pointSum;
    }

}
