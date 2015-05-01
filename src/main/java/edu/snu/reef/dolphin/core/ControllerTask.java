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
import edu.snu.reef.dolphin.groupcomm.interfaces.DataBroadcastSender;
import edu.snu.reef.dolphin.groupcomm.interfaces.DataGatherReceiver;
import edu.snu.reef.dolphin.groupcomm.interfaces.DataReduceReceiver;
import edu.snu.reef.dolphin.groupcomm.interfaces.DataScatterSender;
import edu.snu.reef.dolphin.groupcomm.names.*;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class ControllerTask implements Task {
  public final static String TASK_ID_PREFIX = "CtrlTask";
  private final static Logger LOG = Logger.getLogger(ControllerTask.class.getName());

  private final String taskId;
  private final UserControllerTask userControllerTask;
  private final CommunicationGroupClient commGroup;
  private final Broadcast.Sender<CtrlMessage> ctrlMessageBroadcast;

  @Inject
  public ControllerTask(final GroupCommClient groupCommClient,
                        final UserControllerTask userControllerTask,
                        @Parameter(TaskConfigurationOptions.Identifier.class) String taskId,
                        @Parameter(CommunicationGroup.class) final String commGroupName) throws ClassNotFoundException {
    this.commGroup = groupCommClient.getCommunicationGroup((Class<? extends Name<String>>) Class.forName(commGroupName));
    this.userControllerTask = userControllerTask;
    this.taskId = taskId;
    this.ctrlMessageBroadcast = commGroup.getBroadcastSender(CtrlMsgBroadcast.class);
  }

  @Override
  public final byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, String.format("%s starting...", taskId));

    int iteration = 0;
    userControllerTask.initialize();
    while(!userControllerTask.isTerminated(iteration)) {
      ctrlMessageBroadcast.send(CtrlMessage.RUN);
      sendData(iteration);
      receiveData(iteration);
      userControllerTask.run(iteration);
      updateTopology();
      iteration++;
    }
    ctrlMessageBroadcast.send(CtrlMessage.TERMINATE);
    userControllerTask.cleanup();

    return null;
  }

  /**
   * Update the group communication topology, if it has changed
   */
  private final void updateTopology() {
    if (commGroup.getTopologyChanges().exist()) {
      commGroup.updateTopology();
    }
  }

  private void sendData(int iteration) throws Exception {
    if (userControllerTask.isBroadcastUsed()) {
      commGroup.getBroadcastSender(DataBroadcast.class).send(
          ((DataBroadcastSender) userControllerTask).sendBroadcastData(iteration));
    }
    if (userControllerTask.isScatterUsed()) {
      commGroup.getScatterSender(DataScatter.class).send(
          ((DataScatterSender) userControllerTask).sendScatterData(iteration));
    }
  }

  private void receiveData(int iteration) throws Exception {
    if (userControllerTask.isGatherUsed()) {
      ((DataGatherReceiver)userControllerTask).receiveGatherData(iteration,
          commGroup.getGatherReceiver(DataGather.class).receive());
    }
    if (userControllerTask.isReduceUsed()) {
      ((DataReduceReceiver)userControllerTask).receiveReduceData(iteration,
          commGroup.getReduceReceiver(DataReduce.class).reduce());
    }
  }
}
