package edu.snu.reef.flexion.examples;

import edu.snu.reef.flexion.core.FlexionConfiguration;
import edu.snu.reef.flexion.core.FlexionLauncher;

public final class SimpleREEF {
  public final static void main(String[] args) throws Exception {
    FlexionLauncher.run(FlexionConfiguration.CONF(args)
        .set(FlexionConfiguration.IDENTIFIER, "Simple REEF")
        .set(FlexionConfiguration.CONTROLLER_TASK, SimpleCtrlTask.class)
        .set(FlexionConfiguration.COMPUTE_TASK, SimpleCmpTask.class)
        .set(FlexionConfiguration.INPUT_DIR, "sample")
        .build());
  }
}
