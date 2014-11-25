package edu.snu.reef.flexion.core;

import java.util.List;

public interface UserControllerTask {
  public Integer run();
  public Integer run(Integer dataFromCmpTask);
}
