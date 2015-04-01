package edu.snu.reef.flexion.examples.ml.algorithms.kmeans;

import edu.snu.reef.flexion.core.UserComputeTask;
import edu.snu.reef.flexion.examples.ml.data.Centroid;
import edu.snu.reef.flexion.examples.ml.data.VectorDistanceMeasure;
import edu.snu.reef.flexion.examples.ml.data.VectorSum;
import edu.snu.reef.flexion.groupcomm.interfaces.DataBroadcastReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.DataReduceSender;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.network.util.Pair;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class KMeansMainCmpTask extends UserComputeTask<Pair<List<Vector>, List<Vector>>>
        implements DataBroadcastReceiver<List<Centroid>>, DataReduceSender<Map<Integer, VectorSum>> {

    /**
     * Points read from input data to work on
     */
    private List<Vector> points = null;

    /**
     * Centroids of clusters
     */
    private List<Centroid> centroids = new ArrayList<>();

    /**
     * Vector sum of the points assigned to each cluster
     */
    private Map<Integer, VectorSum> pointSum = new HashMap<>();

    /**
     * Definition of 'distance between points' for this job
     * Default measure is Euclidean distance
     */
    private final VectorDistanceMeasure distanceMeasure;

    /**
     * This class is instantiated by TANG
     * Constructs a single Compute Task for k-means
     * @param distanceMeasure distance measure to use to compute distances between points
     */
    @Inject
    public KMeansMainCmpTask(final VectorDistanceMeasure distanceMeasure) {
        this.distanceMeasure = distanceMeasure;
    }

    @Override
    public void run(int iteration, Pair<List<Vector>, List<Vector>> data) {

        points = data.second;

        // Compute the nearest cluster centroid for each point
        pointSum = new HashMap<>();

        for (final Vector vector : points) {
            double nearestClusterDist = Double.MAX_VALUE;
            int nearestClusterId = -1;

            int clusterId = 0;
            for (Centroid centroid : centroids) {
                final double distance = distanceMeasure.distance(centroid.vector, vector);
                if (nearestClusterDist > distance) {
                    nearestClusterDist = distance;
                    nearestClusterId = clusterId;
                }
                clusterId++;
            }

            // Compute vector sums for each cluster centroid
            if (pointSum.containsKey(nearestClusterId) == false) {
                pointSum.put(nearestClusterId, new VectorSum(vector, 1, true));
            } else {
                pointSum.get(nearestClusterId).add(vector);
            }
        }


    }

    @Override
    public void receiveBroadcastData(List<Centroid> centroids) {
        this.centroids = centroids;
    }

    @Override
    public Map<Integer, VectorSum> sendReduceData(int iteration) {
        return pointSum;
    }
}