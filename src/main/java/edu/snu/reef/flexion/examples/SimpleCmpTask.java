package edu.snu.reef.flexion.examples;

import edu.snu.reef.flexion.core.UserComputeTask;

import javax.inject.Inject;

public final class SimpleCmpTask implements UserComputeTask {
  @Inject
  private SimpleCmpTask() {
  }

  @Override
  public final Integer run(final Integer data) {
    return data + 1;
  }
}
