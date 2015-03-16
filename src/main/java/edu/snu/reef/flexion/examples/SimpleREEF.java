package edu.snu.reef.flexion.examples;

import edu.snu.reef.flexion.core.FlexionConfiguration;
import edu.snu.reef.flexion.core.FlexionLauncher;
import edu.snu.reef.flexion.examples.sgd.SgdCmpTask;
import edu.snu.reef.flexion.examples.sgd.SgdCtrlTask;

public final class SimpleREEF {
  public final static void main(String[] args) throws Exception {
    FlexionLauncher.run(FlexionConfiguration.CONF(args)
        .set(FlexionConfiguration.IDENTIFIER, "Simple REEF")
        .set(FlexionConfiguration.CONTROLLER_TASK, SgdCtrlTask.class)
        .set(FlexionConfiguration.COMPUTE_TASK, SgdCmpTask.class)
        .build());
  }
}
