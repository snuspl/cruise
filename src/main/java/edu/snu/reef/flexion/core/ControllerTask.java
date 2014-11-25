package edu.snu.reef.flexion.core;

import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Logger;

public class ControllerTask implements Task {
  private final static Logger LOG = Logger.getLogger(ControllerTask.class.getName());
  public final static String TASK_ID = "CtrlTask";

  private final UserControllerTask userControllerTask;

  @Inject
  public ControllerTask(final UserControllerTask userControllerTask) {
    this.userControllerTask = userControllerTask;
  }

  @Override
  public final byte[] call(final byte[] memento) throws Exception {
    for (int i = 0; i < 3; i++) {
      final Integer dataToSend = userControllerTask.run();
      // TODO: send(dataToSend);
    }

    return null;
  }
}
