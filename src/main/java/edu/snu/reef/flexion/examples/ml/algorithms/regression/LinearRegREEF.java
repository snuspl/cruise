package edu.snu.reef.flexion.examples.ml.algorithms.regression;

import edu.snu.reef.flexion.core.FlexionConfiguration;
import edu.snu.reef.flexion.core.FlexionLauncher;
import edu.snu.reef.flexion.core.UserJobInfo;
import edu.snu.reef.flexion.core.UserParameters;
import edu.snu.reef.flexion.parameters.JobIdentifier;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;

public class LinearRegREEF {

    public final static void main(String[] args) throws Exception {

        FlexionLauncher.run(
                Configurations.merge(
                        FlexionConfiguration.CONF(args, LinearRegParameters.getCommandLine()),
                        Tang.Factory.getTang().newConfigurationBuilder()
                                .bindNamedParameter(JobIdentifier.class, "Linear Regression")
                                .bindImplementation(UserJobInfo.class, LinearRegJobInfo.class)
                                .bindImplementation(UserParameters.class, LinearRegParameters.class)
                                .build()
                )
        );
    }


}
