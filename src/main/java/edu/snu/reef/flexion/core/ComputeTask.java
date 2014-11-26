package edu.snu.reef.flexion.core;

import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ComputeTask implements Task {
  private final static Logger LOG = Logger.getLogger(ComputeTask.class.getName());
  public final static String TASK_ID = "CmpTask";

  private final FlexionService flexionService;
  private final UserComputeTask userComputeTask;

  @Inject
  public ComputeTask(final FlexionService flexionService,
                     final UserComputeTask userComputeTask) {
    this.flexionService = flexionService;
    this.userComputeTask = userComputeTask;
  }

  @Override
  public final byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, "CmpTask commencing...");

    while (!flexionService.terminate()) {
      Integer data = flexionService.recieve();
      data = userComputeTask.run(data);
      flexionService.send(data);
    }

    return null;
  }
}
