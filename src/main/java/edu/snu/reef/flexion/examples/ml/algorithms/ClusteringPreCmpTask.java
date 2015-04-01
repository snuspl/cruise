package edu.snu.reef.flexion.examples.ml.algorithms;

import edu.snu.reef.flexion.core.UserComputeTask;
import edu.snu.reef.flexion.examples.ml.data.Centroid;
import edu.snu.reef.flexion.groupcomm.interfaces.DataGatherSender;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.network.util.Pair;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

public final class ClusteringPreCmpTask extends UserComputeTask<Pair<List<Vector>, List<Vector>>>
        implements DataGatherSender<List<Vector>> {

    /**
     * Points read from input data to work on
     */
    private List<Vector> points = null;

    /**
     * Initial centroids read from input data to pass to Controller Task
     */
    private List<Vector> initialCentroids = null;

    /**
     * Centroids of clusters
     */
    private List<Centroid> centroids = new ArrayList<Centroid>();

    /**
     * This class is instantiated by TANG
     * Constructs a single Compute Task for k-means
     */
    @Inject
    public ClusteringPreCmpTask() {

    }

    @Override
    public void run(int iteration, Pair<List<Vector>, List<Vector>> data) {
        initialCentroids = data.first;
    }

    @Override
    public List<Vector> sendGatherData(int iteration) {
        return initialCentroids;
    }
}