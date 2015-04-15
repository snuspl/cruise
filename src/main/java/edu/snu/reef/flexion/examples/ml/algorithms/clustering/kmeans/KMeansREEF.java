package edu.snu.reef.flexion.examples.ml.algorithms.clustering.kmeans;

import edu.snu.reef.flexion.core.FlexionConfiguration;
import edu.snu.reef.flexion.core.FlexionLauncher;
import edu.snu.reef.flexion.core.UserJobInfo;
import edu.snu.reef.flexion.core.UserParameters;
import edu.snu.reef.flexion.parameters.JobIdentifier;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;

public class KMeansREEF {

    public final static void main(String[] args) throws Exception {

        FlexionLauncher.run(
                Configurations.merge(
                        FlexionConfiguration.CONF(args, KMeansParameters.getCommandLine()),
                        Tang.Factory.getTang().newConfigurationBuilder()
                                .bindNamedParameter(JobIdentifier.class, "K-means Clustering")
                                .bindImplementation(UserJobInfo.class, KMeansJobInfo.class)
                                .bindImplementation(UserParameters.class, KMeansParameters.class)
                                .build()
                )
        );
    }


}
