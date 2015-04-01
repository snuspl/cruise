package edu.snu.reef.flexion.examples.ml.algorithms;

import edu.snu.reef.flexion.core.KeyValueStore;
import edu.snu.reef.flexion.core.UserControllerTask;
import edu.snu.reef.flexion.examples.ml.data.Centroid;
import edu.snu.reef.flexion.examples.ml.data.VectorSum;
import edu.snu.reef.flexion.examples.ml.key.Centroids;
import edu.snu.reef.flexion.groupcomm.interfaces.DataGatherReceiver;
import org.apache.mahout.math.Vector;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Logger;

public final class ClusteringPreCtrlTask extends UserControllerTask
        implements DataGatherReceiver<List<Vector>> {

    private static final Logger LOG = Logger.getLogger(ClusteringPreCtrlTask.class.getName());

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
     */
    @Inject
    public ClusteringPreCtrlTask() {

    }

    @Override
    public void run(int iteration) {
        //do nothing
    }

    @Override
    public void cleanup(KeyValueStore keyValueStore) {

        int clusterID = 0;
        for (final Vector vector : initialCentroids) {
            centroids.add(new Centroid(clusterID++, vector));
        }

        // pass initial centroids to the main process
        keyValueStore.put(Centroids.class, centroids);
    }

    @Override
    public boolean isTerminated(int iteration) {
        return true;

    }

    @Override
    public void receiveGatherData(List<List<Vector>> initialCentroids) {
        this.initialCentroids = new LinkedList<Vector>();
        // Flatten the given list of lists
        for(List<Vector> list : initialCentroids) {
            for(Vector vector: list){
                this.initialCentroids.add(vector);
            }
        }
    }

}
