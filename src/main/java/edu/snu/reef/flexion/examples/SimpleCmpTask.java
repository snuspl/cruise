package edu.snu.reef.flexion.examples;

import edu.snu.reef.flexion.core.UserComputeTask;

import javax.inject.Inject;

public final class SimpleCmpTask implements UserComputeTask {
  @Inject
  private SimpleCmpTask() {
  }

  @Override
  public final Integer run(final Integer data) {
    float increment = 0;
    for (int i = 0; i < 500000; i++) {
      increment += Math.random();
    }
    return (int) (data + increment);
  }
}
