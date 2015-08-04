/*
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
package edu.snu.cay.dolphin.core;

import edu.snu.cay.dolphin.core.metric.*;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataScatterSender;
import edu.snu.cay.dolphin.groupcomm.names.*;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataBroadcastSender;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataGatherReceiver;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataReduceReceiver;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.group.api.operators.Broadcast;
import org.apache.reef.io.network.group.api.task.CommunicationGroupClient;
import org.apache.reef.io.network.group.api.task.GroupCommClient;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class ControllerTask implements Task {
  public final static String TASK_ID_PREFIX = "CtrlTask";
  private final static Logger LOG = Logger.getLogger(ControllerTask.class.getName());

  private final String taskId;
  private final UserControllerTask userControllerTask;
  private final CommunicationGroupClient commGroup;
  private final Broadcast.Sender<CtrlMessage> ctrlMessageBroadcast;
  private final MetricsCollector metricsCollector;
  private final Set<MetricTracker> metricTrackerSet;
  private final InsertableMetricTracker insertableMetricTracker;

  @Inject
  public ControllerTask(final GroupCommClient groupCommClient,
                        final UserControllerTask userControllerTask,
                        @Parameter(TaskConfigurationOptions.Identifier.class) final String taskId,
                        @Parameter(CommunicationGroup.class) final String commGroupName,
                        final MetricsCollector metricsCollector,
                        @Parameter(MetricTrackers.class) final Set<MetricTracker> metricTrackerSet,
                        final InsertableMetricTracker insertableMetricTracker) throws ClassNotFoundException {
    this.commGroup = groupCommClient.getCommunicationGroup((Class<? extends Name<String>>) Class.forName(commGroupName));
    this.userControllerTask = userControllerTask;
    this.taskId = taskId;
    this.ctrlMessageBroadcast = commGroup.getBroadcastSender(CtrlMsgBroadcast.class);
    this.metricsCollector = metricsCollector;
    this.metricTrackerSet = metricTrackerSet;
    this.insertableMetricTracker = insertableMetricTracker;
  }

  @Override
  public final byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, String.format("%s starting...", taskId));

    userControllerTask.initialize();
    try (final MetricsCollector metricsCollector = this.metricsCollector;) {
      metricsCollector.registerTrackers(metricTrackerSet);
      int iteration = 0;
      while(!userControllerTask.isTerminated(iteration)) {
        metricsCollector.start();
        ctrlMessageBroadcast.send(CtrlMessage.RUN);
        sendData(iteration);
        receiveData(iteration);
        runUserControllerTask(iteration);
        metricsCollector.stop();
        updateTopology();
        iteration++;
      }
      ctrlMessageBroadcast.send(CtrlMessage.TERMINATE);
      userControllerTask.cleanup();
    }

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

  private void runUserControllerTask(final int iteration) throws MetricException {
    insertableMetricTracker.put(DolphinMetricKeys.CONTROLLER_TASK_USER_CONTROLLER_TASK_START, System.currentTimeMillis());
    userControllerTask.run(iteration);
    insertableMetricTracker.put(DolphinMetricKeys.CONTROLLER_TASK_USER_CONTROLLER_TASK_END, System.currentTimeMillis());
  }

  private final void sendData(final int iteration) throws NetworkException, InterruptedException, MetricException {
    insertableMetricTracker.put(DolphinMetricKeys.CONTROLLER_TASK_SEND_DATA_START, System.currentTimeMillis());
    if (userControllerTask.isBroadcastUsed()) {
      commGroup.getBroadcastSender(DataBroadcast.class).send(
          ((DataBroadcastSender) userControllerTask).sendBroadcastData(iteration));
    }
    if (userControllerTask.isScatterUsed()) {
      commGroup.getScatterSender(DataScatter.class).send(
          ((DataScatterSender) userControllerTask).sendScatterData(iteration));
    }
    insertableMetricTracker.put(DolphinMetricKeys.CONTROLLER_TASK_SEND_DATA_END, System.currentTimeMillis());
  }

  private final void receiveData(final int iteration) throws NetworkException, InterruptedException, MetricException {
    insertableMetricTracker.put(DolphinMetricKeys.CONTROLLER_TASK_RECEIVE_DATA_START, System.currentTimeMillis());
    if (userControllerTask.isGatherUsed()) {
      ((DataGatherReceiver)userControllerTask).receiveGatherData(iteration,
          commGroup.getGatherReceiver(DataGather.class).receive());
    }
    if (userControllerTask.isReduceUsed()) {
      ((DataReduceReceiver)userControllerTask).receiveReduceData(iteration,
          commGroup.getReduceReceiver(DataReduce.class).reduce());
    }
    insertableMetricTracker.put(DolphinMetricKeys.CONTROLLER_TASK_RECEIVE_DATA_END, System.currentTimeMillis());
  }
}
