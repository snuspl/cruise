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
package edu.snu.cay.dolphin.bsp.core;

import edu.snu.cay.common.metric.*;
import edu.snu.cay.dolphin.bsp.core.metric.avro.MetricsMessage;
import edu.snu.cay.dolphin.bsp.core.metric.avro.SrcType;
import edu.snu.cay.dolphin.bsp.groupcomm.interfaces.*;
import edu.snu.cay.dolphin.bsp.groupcomm.names.*;
import edu.snu.cay.dolphin.bsp.core.avro.IterationInfo;
import edu.snu.cay.dolphin.bsp.core.metric.avro.ControllerMsg;
import edu.snu.cay.dolphin.bsp.core.sync.ControllerTaskSync;

import edu.snu.cay.utils.trace.HTraceInfoCodec;
import edu.snu.cay.utils.trace.HTraceUtils;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.group.api.operators.Broadcast;
import org.apache.reef.io.network.group.api.task.CommunicationGroupClient;
import org.apache.reef.io.network.group.api.task.GroupCommClient;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.inject.Inject;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.dolphin.bsp.core.DolphinMetricKeys.*;

public final class ControllerTask implements Task {
  public static final String TASK_ID_PREFIX = "CtrlTask";
  private static final Logger LOG = Logger.getLogger(ControllerTask.class.getName());

  private final String taskId;
  private final UserControllerTask userControllerTask;
  private final CommunicationGroupClient commGroup;
  private final Broadcast.Sender<CtrlMessage> ctrlMessageBroadcast;
  private final ControllerTaskSync controllerTaskSync;
  private final MetricsCollector metricsCollector;
  private final Set<MetricTracker> metricTrackerSet;
  private final InsertableMetricTracker insertableMetricTracker;
  private final MetricsHandler metricsHandler;
  private final MetricsMsgSender<MetricsMessage> metricsMsgSender;
  private final HTraceInfoCodec hTraceInfoCodec;
  private final UserTaskTrace userTaskTrace;

  private int iteration = 0;

  @Inject
  public ControllerTask(final GroupCommClient groupCommClient,
                        final UserControllerTask userControllerTask,
                        @Parameter(TaskConfigurationOptions.Identifier.class) final String taskId,
                        @Parameter(CommunicationGroup.class) final String commGroupName,
                        final ControllerTaskSync controllerTaskSync,
                        final MetricsCollector metricsCollector,
                        @Parameter(MetricTrackers.class) final Set<MetricTracker> metricTrackerSet,
                        final InsertableMetricTracker insertableMetricTracker,
                        final MetricsHandler metricsHandler,
                        final MetricsMsgSender<MetricsMessage> metricsMsgSender,
                        final HTraceInfoCodec hTraceInfoCodec,
                        final UserTaskTrace userTaskTrace) throws ClassNotFoundException {
    this.commGroup =
        groupCommClient.getCommunicationGroup((Class<? extends Name<String>>) Class.forName(commGroupName));
    this.userControllerTask = userControllerTask;
    this.taskId = taskId;
    this.ctrlMessageBroadcast = commGroup.getBroadcastSender(CtrlMsgBroadcast.class);
    this.controllerTaskSync = controllerTaskSync;
    this.metricsCollector = metricsCollector;
    this.metricTrackerSet = metricTrackerSet;
    this.insertableMetricTracker = insertableMetricTracker;
    this.metricsHandler = metricsHandler;
    this.metricsMsgSender = metricsMsgSender;
    this.hTraceInfoCodec = hTraceInfoCodec;
    this.userTaskTrace = userTaskTrace;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, String.format("%s starting...", taskId));
    final TraceInfo taskTraceInfo = memento == null ? null :
        HTraceUtils.fromAvro(hTraceInfoCodec.decode(memento));

    initializeGroupCommOperators();
    userControllerTask.initialize();
    try (MetricsCollector metricsCollector = this.metricsCollector;) {
      metricsCollector.registerTrackers(metricTrackerSet);
      while (!userControllerTask.isTerminated(iteration)) {
        try (TraceScope traceScope = Trace.startSpan("pause-" + iteration, taskTraceInfo)) {
          if (controllerTaskSync.update(getIterationInfo())) {
            updateTopology();
          }
        }

        try (TraceScope traceScope = Trace.startSpan("iter-" + iteration, taskTraceInfo)) {
          userTaskTrace.setParentTraceInfo(TraceInfo.fromSpan(traceScope.getSpan()));
          metricsCollector.start();
          ctrlMessageBroadcast.send(CtrlMessage.RUN);
          sendData();
          receiveData();
          runUserControllerTask();
          metricsCollector.stop();
          sendMetrics();
        }
        iteration++;
      }
      ctrlMessageBroadcast.send(CtrlMessage.TERMINATE);
      userControllerTask.cleanup();
    }

    return null;
  }

  /**
   * Update the group communication topology, if it has changed.
   */
  private void updateTopology() {
    if (commGroup.getTopologyChanges().exist()) {
      commGroup.updateTopology();
    }
  }

  private void initializeGroupCommOperators() {
    if (userControllerTask.isBroadcastUsed()) {
      commGroup.getBroadcastSender(DataBroadcast.class);
    }
    if (userControllerTask.isScatterUsed()) {
      commGroup.getScatterSender(DataScatter.class);
    }
    if (userControllerTask.isGatherUsed()) {
      commGroup.getGatherReceiver(DataGather.class);
    }
    if (userControllerTask.isReduceUsed()) {
      commGroup.getReduceReceiver(DataReduce.class);
    }
  }

  private void runUserControllerTask() throws MetricException {
    insertableMetricTracker.put(CONTROLLER_TASK_USER_CONTROLLER_TASK_START, System.currentTimeMillis());
    userControllerTask.run(iteration);
    insertableMetricTracker.put(CONTROLLER_TASK_USER_CONTROLLER_TASK_END, System.currentTimeMillis());
  }

  private void sendData() throws NetworkException, InterruptedException, MetricException {
    insertableMetricTracker.put(CONTROLLER_TASK_SEND_DATA_START, System.currentTimeMillis());
    if (userControllerTask.isBroadcastUsed()) {
      commGroup.getBroadcastSender(DataBroadcast.class).send(
          ((DataBroadcastSender)userControllerTask).sendBroadcastData(iteration));
    }
    if (userControllerTask.isScatterUsed()) {
      commGroup.getScatterSender(DataScatter.class).send(
          ((DataScatterSender)userControllerTask).sendScatterData(iteration));
    }
    insertableMetricTracker.put(CONTROLLER_TASK_SEND_DATA_END, System.currentTimeMillis());
  }

  private void receiveData() throws NetworkException, InterruptedException, MetricException {
    insertableMetricTracker.put(CONTROLLER_TASK_RECEIVE_DATA_START, System.currentTimeMillis());
    if (userControllerTask.isGatherUsed()) {
      ((DataGatherReceiver)userControllerTask).receiveGatherData(iteration,
          commGroup.getGatherReceiver(DataGather.class).receive());
    }
    if (userControllerTask.isReduceUsed()) {
      ((DataReduceReceiver)userControllerTask).receiveReduceData(iteration,
          commGroup.getReduceReceiver(DataReduce.class).reduce());
    }
    insertableMetricTracker.put(CONTROLLER_TASK_RECEIVE_DATA_END, System.currentTimeMillis());
  }

  private void sendMetrics() {
    final MetricsMessage message = MetricsMessage.newBuilder()
        .setSrcType(SrcType.Controller)
        .setMetrics(metricsHandler.getMetrics())
        .setIterationInfo(getIterationInfo())
        .setControllerMsg(getControllerMsg())
        .build();
    metricsMsgSender.send(message);
  }

  private IterationInfo getIterationInfo() {
    final IterationInfo iterationInfo = IterationInfo.newBuilder()
        .setIteration(iteration)
        .setCommGroupName(commGroup.getName().getName())
        .build();

    LOG.log(Level.FINE, "iterationInfo {0}", iterationInfo);
    return iterationInfo;
  }

  private ControllerMsg getControllerMsg() {
    final ControllerMsg controllerMsg = ControllerMsg.newBuilder()
        .build();
    LOG.log(Level.FINE, "controllerMsg {0}", controllerMsg);
    return controllerMsg;
  }
}
