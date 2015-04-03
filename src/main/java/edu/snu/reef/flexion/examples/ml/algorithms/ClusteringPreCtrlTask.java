package edu.snu.reef.flexion.examples.ml.algorithms;

import edu.snu.reef.flexion.core.KeyValueStore;
import edu.snu.reef.flexion.core.UserControllerTask;
import edu.snu.reef.flexion.examples.ml.data.Centroid;
import edu.snu.reef.flexion.examples.ml.key.Centroids;
import edu.snu.reef.flexion.examples.ml.parameters.NumberOfClusters;
import edu.snu.reef.flexion.groupcomm.interfaces.DataGatherReceiver;
import org.apache.mahout.math.Vector;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public final class ClusteringPreCtrlTask extends UserControllerTask
        implements DataGatherReceiver<List<Vector>> {

    private static final Logger LOG = Logger.getLogger(ClusteringPreCtrlTask.class.getName());

    /**
     * Number of clusters
     */
    private final int numberOfClusters;

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
    public ClusteringPreCtrlTask(@Parameter(NumberOfClusters.class) final int numberOfClusters) {
        this.numberOfClusters = numberOfClusters;
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

        List<Vector> points = new LinkedList<>();

        // Flatten the given list of lists
        for(List<Vector> list : initialCentroids) {
            for(Vector vector: list){
                points.add(vector);
            }
        }

        //sample initial centroids
        this.initialCentroids = ClusteringPreCmpTask.sample(points, numberOfClusters);

    }

}
