package edu.snu.reef.flexion.examples;

import edu.snu.reef.flexion.core.FlexionConfiguration;
import edu.snu.reef.flexion.core.FlexionLauncher;

public final class SimpleREEF {
  public final static void main(String[] args) {
    FlexionLauncher.run(FlexionConfiguration.CONF
        .set(FlexionConfiguration.IDENTIFIER, "Simple REEF")
        .set(FlexionConfiguration.EVALUATOR_NUM, 5)
        .set(FlexionConfiguration.CONTROLLER_TASK, SimpleCtrlTask.class)
        .set(FlexionConfiguration.COMPUTE_TASK, SimpleCmpTask.class)
        .set(FlexionConfiguration.ON_LOCAL, true)
        .set(FlexionConfiguration.EVALUATOR_SIZE, 256)
        .set(FlexionConfiguration.INPUT_DIR, "file:///home/jsjason/Projects/flexion/bin/sample")
        .build());
  }
}
