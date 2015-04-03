package edu.snu.reef.flexion.examples.ml.algorithms;

import edu.snu.reef.flexion.core.UserComputeTask;
import edu.snu.reef.flexion.examples.ml.parameters.NumberOfClusters;
import edu.snu.reef.flexion.groupcomm.interfaces.DataGatherSender;
import org.apache.mahout.math.Vector;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public final class ClusteringPreCmpTask extends UserComputeTask<List<Vector>>
        implements DataGatherSender<List<Vector>> {

    /**
     * Number of clusters
     */
    private final int numberOfClusters;

    /**
     * Points read from input data to work on
     */
    private List<Vector> points = null;

    /**
     * Sampled points
     */
    private List<Vector> samples = new LinkedList<>();

    /**
     * This class is instantiated by TANG
     * Constructs a single Compute Task for k-means
     * @param numberOfClusters
     */
    @Inject
    public ClusteringPreCmpTask(@Parameter(NumberOfClusters.class) final int numberOfClusters) {
        this.numberOfClusters = numberOfClusters;
    }

    @Override
    public void run(int iteration, List<Vector> data) {
        points = data;

        //randomly sample points so that the number of points are equal to that of clusters
        samples = sample(points, numberOfClusters);

    }

    @Override
    public List<Vector> sendGatherData(int iteration) {
        return samples;
    }

    /**
     * Random Sampling
     * @param points
     * @param maxNumOfSamples
     * @return
     */
    static List<Vector> sample(List<Vector> points, int maxNumOfSamples){

        List<Vector> samples = new LinkedList<>();

        if(points.isEmpty()) {
            return samples;
        }

        Random random = new Random();
        int numberOfPoints = points.size();
        int numberOfSamples = Math.min(maxNumOfSamples, numberOfPoints);
        Vector[] pointArray = points.toArray(new Vector[0]);

        for(int i=0; i<numberOfSamples; i++){
            int index = random.nextInt(numberOfPoints-1-i);
            samples.add(pointArray[index]);
            pointArray[index] = pointArray[numberOfPoints-1-i];
        }

        return samples;
    }


}