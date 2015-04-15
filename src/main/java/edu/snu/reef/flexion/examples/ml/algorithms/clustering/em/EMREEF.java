package edu.snu.reef.flexion.examples.ml.algorithms.clustering.em;

import edu.snu.reef.flexion.core.FlexionConfiguration;
import edu.snu.reef.flexion.core.FlexionLauncher;
import edu.snu.reef.flexion.core.UserJobInfo;
import edu.snu.reef.flexion.core.UserParameters;
import edu.snu.reef.flexion.parameters.JobIdentifier;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;

public class EMREEF {

  public final static void main(String[] args) throws Exception {

    FlexionLauncher.run(
        Configurations.merge(
            FlexionConfiguration.CONF(args, EMParameters.getCommandLine()),
            Tang.Factory.getTang().newConfigurationBuilder()
                .bindNamedParameter(JobIdentifier.class, "EM Clustering")
                .bindImplementation(UserJobInfo.class, EMJobInfo.class)
                .bindImplementation(UserParameters.class, EMParameters.class)
                .build()
        )
    );
  }


}
