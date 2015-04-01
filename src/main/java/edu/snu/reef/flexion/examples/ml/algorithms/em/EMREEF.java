package edu.snu.reef.flexion.examples.ml.algorithms.em;

import edu.snu.reef.flexion.core.FlexionConfiguration;
import edu.snu.reef.flexion.core.FlexionLauncher;

public class EMREEF {

    public final static void main(String[] args) throws Exception {

        FlexionLauncher.run(FlexionConfiguration.CONF(args, EMParameters.getCommandLine())
                .set(FlexionConfiguration.IDENTIFIER, "EM Clustering")
                .set(FlexionConfiguration.JOB_INFO, EMJobInfo.class)
                .set(FlexionConfiguration.PARAMETERS, EMParameters.class)
                .build());
    }


}
