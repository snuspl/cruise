package edu.snu.reef.flexion.examples.ml.data;

import java.util.Formatter;
import java.util.Locale;

/**
 * This class represents a summary of the cluster
 * The summary includes (1) prior probability, (2) the centroid, and (3) the covariance matrix
 */
public final class ClusterSummary {

    private final int clusterID;
    private final double prior;
    private final Centroid centroid;
    private final Covariance covariance;


    public ClusterSummary(int clusterID, double prior, Centroid centroid, Covariance covariance) {
        this.clusterID = clusterID;
        this.prior = prior;
        this.centroid = centroid;
        this.covariance = covariance;
    }

    public final int getClusterID() { return clusterID; }

    public final double getPrior() {
        return prior;
    }

    public final Centroid getCentroid() {
        return centroid;
    }

    public final Covariance getCovariance() {
        return covariance;
    }

    @Override
    public String toString() {
        final StringBuilder b = new StringBuilder("Cluster Summary:\n");
        try (final Formatter formatter = new Formatter(b, Locale.US)) {
            formatter.format("Prior probability: %f\n, ", prior);
            formatter.format("Centroid: ");
            for (int i = 0; i < centroid.vector.size(); ++i) {
                formatter.format("%1.3f, ", centroid.vector.get(i));
            }
            formatter.format("\n");
            formatter.format("Covariance:, ");
            for (int i = 0; i < covariance.getMatrix().rowSize(); ++i) {
                for (int j = 0; j < covariance.getMatrix().columnSize(); ++j) {
                        formatter.format("%1.3f, ", covariance.getMatrix().get(i, j));
                }
                formatter.format("\n");
            }
            formatter.format("\n");
        }
        return b.toString();
    }

}
