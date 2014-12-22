package edu.snu.reef.flexion.core;

import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.task.Task;
import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ComputeTask implements Task, TaskMessageSource {
  private final static Logger LOG = Logger.getLogger(ComputeTask.class.getName());
  public final static String TASK_ID = "CmpTask";

  private final FlexionService flexionService;
  private final UserComputeTask userComputeTask;

  private final ObjectSerializableCodec<Long> codecLong = new ObjectSerializableCodec<>();

  private long runTime = -1;

  @Inject
  public ComputeTask(final GroupCommClient groupCommClient,
                     final DataSet dataSet,
                     final UserComputeTask userComputeTask,
                     final HeartBeatTriggerManager heartBeatTriggerManager) {
    this.flexionService = new FlexionService(dataSet, groupCommClient, heartBeatTriggerManager);
    this.userComputeTask = userComputeTask;
  }

  @Override
  public final byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, "CmpTask commencing...");

    while (!flexionService.terminate()) {
      Integer data = flexionService.recieve();
      final long runStart = System.currentTimeMillis();
      data = userComputeTask.run(data);
      runTime = System.currentTimeMillis() - runStart;
      flexionService.send(data);
    }

    return null;
  }

  @Override
  public synchronized Optional<TaskMessage> getMessage() {
    return Optional.of(TaskMessage.from(FlexionServiceCmp.class.getName(),
        this.codecLong.encode(this.runTime)));
  }
}
