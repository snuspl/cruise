/**
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.reef.dolphin.core;

import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import edu.snu.reef.dolphin.groupcomm.interfaces.DataBroadcastReceiver;
import edu.snu.reef.dolphin.groupcomm.interfaces.DataGatherSender;
import edu.snu.reef.dolphin.groupcomm.interfaces.DataReduceSender;
import edu.snu.reef.dolphin.groupcomm.interfaces.DataScatterReceiver;
import edu.snu.reef.dolphin.groupcomm.names.*;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.task.Task;
import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class ComputeTask implements Task, TaskMessageSource {
  public final static String TASK_ID_PREFIX = "CmpTask";
  private final static Logger LOG = Logger.getLogger(ComputeTask.class.getName());

  private final String taskId;
  private final UserComputeTask userComputeTask;
  private final CommunicationGroupClient commGroup;
  private final HeartBeatTriggerManager heartBeatTriggerManager;
  private final Broadcast.Receiver<CtrlMessage> ctrlMessageBroadcast;
  private final ObjectSerializableCodec<Long> codecLong = new ObjectSerializableCodec<>();
  private long runTime = -1;

  @Inject
  public ComputeTask(final GroupCommClient groupCommClient,
                     final UserComputeTask userComputeTask,
                     @Parameter(TaskConfigurationOptions.Identifier.class) String taskId,
                     @Parameter(CommunicationGroup.class) final String commGroupName,
                     final HeartBeatTriggerManager heartBeatTriggerManager) throws ClassNotFoundException {
    this.userComputeTask = userComputeTask;
    this.taskId = taskId;
    this.commGroup = groupCommClient.getCommunicationGroup((Class<? extends Name<String>>) Class.forName(commGroupName));
    this.ctrlMessageBroadcast = commGroup.getBroadcastReceiver(CtrlMsgBroadcast.class);
    this.heartBeatTriggerManager = heartBeatTriggerManager;
  }

  @Override
  public final byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, String.format("%s starting...", taskId));

    userComputeTask.initialize();
    int iteration=0;
    while (!isTerminated()) {
      receiveData(iteration);
      final long runStart = System.currentTimeMillis();
      userComputeTask.run(iteration);
      runTime = System.currentTimeMillis() - runStart;
      sendData(iteration);
      heartBeatTriggerManager.triggerHeartBeat();
      iteration++;
    }
    userComputeTask.cleanup();

    return null;
  }

  private void receiveData(int iteration) throws Exception {
    if (userComputeTask.isBroadcastUsed()) {
      ((DataBroadcastReceiver)userComputeTask).receiveBroadcastData(iteration,
          commGroup.getBroadcastReceiver(DataBroadcast.class).receive());
    }
    if (userComputeTask.isScatterUsed()) {
      ((DataScatterReceiver)userComputeTask).receiveScatterData(iteration,
          commGroup.getScatterReceiver(DataScatter.class).receive());
    }
  }

  private void sendData(int iteration) throws Exception {
    if (userComputeTask.isGatherUsed()) {
      commGroup.getGatherSender(DataGather.class).send(
          ((DataGatherSender)userComputeTask).sendGatherData(iteration));
    }
    if (userComputeTask.isReduceUsed()) {
      commGroup.getReduceSender(DataReduce.class).send(
          ((DataReduceSender)userComputeTask).sendReduceData(iteration));
    }
  }

  private boolean isTerminated() throws Exception {
    return ctrlMessageBroadcast.receive() == CtrlMessage.TERMINATE;
  }

  @Override
  public synchronized Optional<TaskMessage> getMessage() {
    return Optional.of(TaskMessage.from(ComputeTask.class.getName(),
        this.codecLong.encode(this.runTime)));
  }
}
