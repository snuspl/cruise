package edu.snu.reef.flexion.examples;

import edu.snu.reef.flexion.core.UserControllerTask;

import javax.inject.Inject;

public final class SimpleCtrlTask implements UserControllerTask {

  @Inject
  private SimpleCtrlTask() {
  }

  @Override
  public final Integer run() {
    return 1;
  }

  @Override
  public final Integer run(final Integer data) {
    return data * 2;
  }
}
