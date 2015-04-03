package edu.snu.reef.flexion.examples.ml.algorithms.kmeans;

import edu.snu.reef.flexion.core.KeyValueStore;
import edu.snu.reef.flexion.core.UserControllerTask;
import edu.snu.reef.flexion.examples.ml.converge.ConvergenceCondition;
import edu.snu.reef.flexion.examples.ml.data.Centroid;
import edu.snu.reef.flexion.examples.ml.data.VectorSum;
import edu.snu.reef.flexion.examples.ml.key.Centroids;
import edu.snu.reef.flexion.examples.ml.parameters.MaxIterations;
import edu.snu.reef.flexion.groupcomm.interfaces.DataBroadcastSender;
import edu.snu.reef.flexion.groupcomm.interfaces.DataReduceReceiver;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class KMeansMainCtrlTask extends UserControllerTask
        implements DataReduceReceiver<Map<Integer, VectorSum>>, DataBroadcastSender<List<Centroid>> {

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
    private List<Centroid> centroids = new ArrayList<>();

    /**
     * This class is instantiated by TANG
     *
     * Constructs the Controller Task for k-means
     *
     * @param convergenceCondition conditions for checking convergence of algorithm
     * @param maxIterations maximum number of iterations allowed before job stops
     */
    @Inject
    public KMeansMainCtrlTask(final ConvergenceCondition convergenceCondition,
                              @Parameter(MaxIterations.class) final int maxIterations) {

        this.convergenceCondition = convergenceCondition;
        this.maxIterations = maxIterations;
    }

    /**
     * Receive initial centroids from the preprocess task
     * @param keyValueStore
     */
    public void initialize(KeyValueStore keyValueStore) {
        centroids = keyValueStore.get(Centroids.class);
    }

    @Override
    public void run(int iteration) {

        for (final Integer id : pointSum.keySet()) {
            final VectorSum vectorSum = pointSum.get(id);
            final Centroid newCentroid = new Centroid(id, vectorSum.computeVectorMean());
            centroids.set(id, newCentroid);
        }

        LOG.log(Level.INFO, "********* Centroids after {0} iterations*********", iteration);
        LOG.log(Level.INFO, "" + centroids);
        LOG.log(Level.INFO, "********* Centroids after {0} iterations*********", iteration);

    }

    @Override
    public boolean isTerminated(int iteration) {
        return convergenceCondition.checkConvergence(centroids)
            || (iteration > maxIterations); // First two iterations are used for initialization

    }

    @Override
    public List<Centroid> sendBroadcastData(int iteration) {
        return centroids;
    }

    @Override
    public void receiveReduceData(Map<Integer, VectorSum> pointSum) {
        this.pointSum = pointSum;
    }

}
