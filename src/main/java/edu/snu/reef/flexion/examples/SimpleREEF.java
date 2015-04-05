package edu.snu.reef.flexion.examples;

import edu.snu.reef.flexion.core.FlexionConfiguration;
import edu.snu.reef.flexion.core.FlexionLauncher;
import edu.snu.reef.flexion.core.UserJobInfo;
import edu.snu.reef.flexion.parameters.JobIdentifier;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;

public final class SimpleREEF {
    public final static void main(String[] args) throws Exception {
        FlexionLauncher.run(
                Configurations.merge(
                        FlexionConfiguration.CONF(args),
                        Tang.Factory.getTang().newConfigurationBuilder()
                                .bindNamedParameter(JobIdentifier.class, "Simple REEF")
                                .bindImplementation(UserJobInfo.class, SimpleJobInfo.class)
                                .build()
                )
        );
    }
}
