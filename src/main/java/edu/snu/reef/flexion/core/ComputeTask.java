package edu.snu.reef.flexion.core;

import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Logger;

public class ComputeTask implements Task {
  private final static Logger LOG = Logger.getLogger(ComputeTask.class.getName());
  public final static String TASK_ID = "CmpTask";

  private final UserComputeTask userComputeTask;

  @Inject
  public ComputeTask(final UserComputeTask userComputeTask) {
    this.userComputeTask = userComputeTask;
  }

  @Override
  public final byte[] call(final byte[] memento) throws Exception {
    // TODO: final Integer dataFromCtrlTask = receive;
    final Integer dataToSend = userComputeTask.run(dataFromCtrlTask);
    // TODO: send(dataToSend);
  }

}
