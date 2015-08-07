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
import edu.snu.cay.dolphin.groupcomm.interfaces.DataBroadcastReceiver;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataGatherSender;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataReduceSender;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataScatterReceiver;
import edu.snu.cay.dolphin.groupcomm.names.*;
import static edu.snu.cay.dolphin.core.DolphinMetricKeys.COMPUTE_TASK_RECEIVE_DATA_START;
import static edu.snu.cay.dolphin.core.DolphinMetricKeys.COMPUTE_TASK_RECEIVE_DATA_END;
import static edu.snu.cay.dolphin.core.DolphinMetricKeys.COMPUTE_TASK_SEND_DATA_START;
import static edu.snu.cay.dolphin.core.DolphinMetricKeys.COMPUTE_TASK_SEND_DATA_END;
import static edu.snu.cay.dolphin.core.DolphinMetricKeys.COMPUTE_TASK_USER_COMPUTE_TASK_START;
import static edu.snu.cay.dolphin.core.DolphinMetricKeys.COMPUTE_TASK_USER_COMPUTE_TASK_END;
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

public final class ComputeTask implements Task {
  public static final String TASK_ID_PREFIX = "CmpTask";
  private static final Logger LOG = Logger.getLogger(ComputeTask.class.getName());

  private final String taskId;
  private final UserComputeTask userComputeTask;
  private final CommunicationGroupClient commGroup;
  private final Broadcast.Receiver<CtrlMessage> ctrlMessageBroadcast;
  private final MetricsCollector metricsCollector;
  private final Set<MetricTracker> metricTrackerSet;
  private final InsertableMetricTracker insertableMetricTracker;

  @Inject
  public ComputeTask(final GroupCommClient groupCommClient,
                     final UserComputeTask userComputeTask,
                     @Parameter(TaskConfigurationOptions.Identifier.class) final String taskId,
                     @Parameter(CommunicationGroup.class) final String commGroupName,
                     final MetricsCollector metricsCollector,
                     @Parameter(MetricTrackers.class) final Set<MetricTracker> metricTrackerSet,
                     final InsertableMetricTracker insertableMetricTracker) throws ClassNotFoundException {
    this.userComputeTask = userComputeTask;
    this.taskId = taskId;
    this.commGroup =
        groupCommClient.getCommunicationGroup((Class<? extends Name<String>>) Class.forName(commGroupName));
    this.ctrlMessageBroadcast = commGroup.getBroadcastReceiver(CtrlMsgBroadcast.class);
    this.metricsCollector = metricsCollector;
    this.metricTrackerSet = metricTrackerSet;
    this.insertableMetricTracker = insertableMetricTracker;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, String.format("%s starting...", taskId));

    userComputeTask.initialize();
    try (final MetricsCollector metricsCollector = this.metricsCollector) {
      metricsCollector.registerTrackers(metricTrackerSet);
      int iteration = 0;
      while (!isTerminated()) {
        metricsCollector.start();
        receiveData(iteration);
        runUserComputeTask(iteration);
        sendData(iteration);
        metricsCollector.stop();
        iteration++;
      }
      userComputeTask.cleanup();
    }

    return null;
  }

  private void runUserComputeTask(final int iteration) throws MetricException {
    insertableMetricTracker.put(COMPUTE_TASK_USER_COMPUTE_TASK_START, System.currentTimeMillis());
    userComputeTask.run(iteration);
    insertableMetricTracker.put(COMPUTE_TASK_USER_COMPUTE_TASK_END, System.currentTimeMillis());
  }

  private void receiveData(final int iteration) throws NetworkException, InterruptedException, MetricException {
    insertableMetricTracker.put(COMPUTE_TASK_RECEIVE_DATA_START, System.currentTimeMillis());
    if (userComputeTask.isBroadcastUsed()) {
      ((DataBroadcastReceiver)userComputeTask).receiveBroadcastData(iteration,
          commGroup.getBroadcastReceiver(DataBroadcast.class).receive());
    }
    if (userComputeTask.isScatterUsed()) {
      ((DataScatterReceiver)userComputeTask).receiveScatterData(iteration,
          commGroup.getScatterReceiver(DataScatter.class).receive());
    }
    insertableMetricTracker.put(COMPUTE_TASK_RECEIVE_DATA_END, System.currentTimeMillis());
  }

  private void sendData(final int iteration) throws NetworkException, InterruptedException, MetricException {
    insertableMetricTracker.put(COMPUTE_TASK_SEND_DATA_START, System.currentTimeMillis());
    if (userComputeTask.isGatherUsed()) {
      commGroup.getGatherSender(DataGather.class).send(
          ((DataGatherSender)userComputeTask).sendGatherData(iteration));
    }
    if (userComputeTask.isReduceUsed()) {
      commGroup.getReduceSender(DataReduce.class).send(
          ((DataReduceSender)userComputeTask).sendReduceData(iteration));
    }
    insertableMetricTracker.put(COMPUTE_TASK_SEND_DATA_END, System.currentTimeMillis());
  }

  private boolean isTerminated() throws Exception {
    return ctrlMessageBroadcast.receive() == CtrlMessage.TERMINATE;
  }
}
