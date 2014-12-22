package edu.snu.reef.flexion.core;

import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ControllerTask implements Task {
  private final static Logger LOG = Logger.getLogger(ControllerTask.class.getName());
  public final static String TASK_ID = "CtrlTask";

  private final FlexionService flexionService;
  private final UserControllerTask userControllerTask;

  @Inject
  public ControllerTask(final GroupCommClient groupCommClient,
                        final UserControllerTask userControllerTask) {
    this.flexionService = new FlexionService(groupCommClient);
    this.userControllerTask = userControllerTask;
  }

  @Override
  public final byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, "CtrlTask commencing...");

    Integer data = userControllerTask.run();
    for (int loopCount = 1; loopCount < 3; loopCount++) {
      flexionService.send(data);
      data = flexionService.recieve();
      data = userControllerTask.run(data);
    }
    flexionService.terminate();

    return null;
  }
}
