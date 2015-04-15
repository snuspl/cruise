package edu.snu.reef.flexion.examples.ml.algorithms.classification;

import edu.snu.reef.flexion.core.FlexionConfiguration;
import edu.snu.reef.flexion.core.FlexionLauncher;
import edu.snu.reef.flexion.core.UserJobInfo;
import edu.snu.reef.flexion.core.UserParameters;
import edu.snu.reef.flexion.parameters.JobIdentifier;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;

public class LogisticRegREEF {

    public final static void main(String[] args) throws Exception {

        FlexionLauncher.run(
                Configurations.merge(
                        FlexionConfiguration.CONF(args, LogisticRegParameters.getCommandLine()),
                        Tang.Factory.getTang().newConfigurationBuilder()
                                .bindNamedParameter(JobIdentifier.class, "Logistic Regression")
                                .bindImplementation(UserJobInfo.class, LogisticRegJobInfo.class)
                                .bindImplementation(UserParameters.class, LogisticRegParameters.class)
                                .build()
                )
        );
    }


}
