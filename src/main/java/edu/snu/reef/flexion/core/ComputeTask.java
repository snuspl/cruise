package edu.snu.reef.flexion.core;

import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import org.apache.reef.io.data.loading.api.DataSet;
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
  public ComputeTask(final GroupCommClient groupCommClient,
                     final DataSet dataSet,
                     final UserComputeTask userComputeTask) {
    this.flexionService = new FlexionService(dataSet ,groupCommClient);
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
