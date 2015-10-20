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

import edu.snu.cay.dolphin.core.avro.IterationInfo;
import edu.snu.cay.dolphin.core.metric.*;
import edu.snu.cay.dolphin.core.metric.avro.ComputeMsg;
import edu.snu.cay.dolphin.core.metric.avro.DataInfo;
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
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.utils.trace.HTraceInfoCodec;
import edu.snu.cay.utils.trace.HTraceUtils;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.group.api.operators.Broadcast;
import org.apache.reef.io.network.group.api.task.CommunicationGroupClient;
import org.apache.reef.io.network.group.api.task.GroupCommClient;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.task.Task;
import org.apache.reef.task.events.CloseEvent;
import org.apache.reef.wake.EventHandler;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
public final class ComputeTask implements Task {
  public static final String TASK_ID_PREFIX = "CmpTask";
  private static final Logger LOG = Logger.getLogger(ComputeTask.class.getName());

  private final String taskId;
  private final MemoryStore memoryStore;
  private final UserComputeTask userComputeTask;
  private final CommunicationGroupClient commGroup;
  private final Broadcast.Receiver<CtrlMessage> ctrlMessageBroadcast;
  private final MetricsCollector metricsCollector;
  private final Set<MetricTracker> metricTrackerSet;
  private final InsertableMetricTracker insertableMetricTracker;
  private final MetricsMessageSender metricsMessageSender;
  private final HTraceInfoCodec hTraceInfoCodec;
  private final UserTaskTrace userTaskTrace;

  private final CountDownLatch terminated = new CountDownLatch(1);
  private final AtomicBoolean isClosing = new AtomicBoolean(false);
  private int iteration;

  @Inject
  public ComputeTask(final GroupCommClient groupCommClient,
                     final MemoryStore memoryStore,
                     final UserComputeTask userComputeTask,
                     @Parameter(TaskConfigurationOptions.Identifier.class) final String taskId,
                     @Parameter(CommunicationGroup.class) final String commGroupName,
                     @Parameter(Iteration.class) final int startIteration,
                     final MetricsCollector metricsCollector,
                     @Parameter(MetricTrackers.class) final Set<MetricTracker> metricTrackerSet,
                     final InsertableMetricTracker insertableMetricTracker,
                     final MetricsMessageSender metricsMessageSender,
                     final HTraceInfoCodec hTraceInfoCodec,
                     final UserTaskTrace userTaskTrace) throws ClassNotFoundException {
    this.memoryStore = memoryStore;
    this.userComputeTask = userComputeTask;
    this.taskId = taskId;
    this.commGroup =
        groupCommClient.getCommunicationGroup((Class<? extends Name<String>>) Class.forName(commGroupName));
    this.iteration = startIteration;
    this.ctrlMessageBroadcast = commGroup.getBroadcastReceiver(CtrlMsgBroadcast.class);
    this.metricsCollector = metricsCollector;
    this.metricTrackerSet = metricTrackerSet;
    this.insertableMetricTracker = insertableMetricTracker;
    this.metricsMessageSender = metricsMessageSender;
    this.hTraceInfoCodec = hTraceInfoCodec;
    this.userTaskTrace = userTaskTrace;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, String.format("%s starting...", taskId));
    final TraceInfo taskTraceInfo = memento == null ? null :
        HTraceUtils.fromAvro(hTraceInfoCodec.decode(memento));

    userComputeTask.initialize();

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final Future<?> result = executor.submit(new Runnable() {
      @Override
      public void run() {
        try (final MetricsCollector collector = metricsCollector) {
          collector.registerTrackers(metricTrackerSet);
          while (!isTerminated()) {
            try (final TraceScope traceScope = Trace.startSpan("iter-" + iteration, taskTraceInfo)) {
              userTaskTrace.setParentTraceInfo(TraceInfo.fromSpan(traceScope.getSpan()));
              collector.start();
              receiveData();
              runUserComputeTask();
              sendData();
              collector.stop();
              sendMetrics();
            }
            iteration++;
          }
          userComputeTask.cleanup();
        } catch (final Exception e) {
          throw new RuntimeException(e);
        } finally {
          terminated.countDown();
        }
      }
    });

    terminated.await();
    executor.shutdownNow();
    try {
      result.get();
    } catch (final ExecutionException e) {
      if (isClosing.get()) {
        LOG.log(Level.INFO, "Task closed by TaskCloseHandler.");
      } else {
        throw new Exception(e);
      }
    }
    return null;
  }

  private void runUserComputeTask() throws MetricException {
    insertableMetricTracker.put(COMPUTE_TASK_USER_COMPUTE_TASK_START, System.currentTimeMillis());
    userComputeTask.run(iteration);
    insertableMetricTracker.put(COMPUTE_TASK_USER_COMPUTE_TASK_END, System.currentTimeMillis());
  }

  private void receiveData() throws NetworkException, InterruptedException, MetricException {
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

  private void sendData() throws NetworkException, InterruptedException, MetricException {
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

  private void sendMetrics() {
    metricsMessageSender
        .setIterationInfo(getIterationInfo())
        .setComputeMsg(getComputeMsg())
        .send();
  }

  private IterationInfo getIterationInfo() {
    final IterationInfo iterationInfo = IterationInfo.newBuilder()
        .setIteration(iteration)
        .setCommGroupName(commGroup.getName().getName())
        .build();

    LOG.log(Level.INFO, "iterationInfo {0}", iterationInfo);
    return iterationInfo;
  }

  private ComputeMsg getComputeMsg() {
    final List<DataInfo> dataInfos = new ArrayList<>();
    for (final String dataType : memoryStore.getElasticStore().getDataTypes()) {
      dataInfos.add(
          DataInfo.newBuilder()
              .setDataType(dataType)
              .setNumUnits(memoryStore.getElasticStore().getNumUnits(dataType))
              .build());
    }

    final ComputeMsg computeMsg = ComputeMsg.newBuilder()
        .setDataInfos(dataInfos)
        .build();
    LOG.log(Level.INFO, "computeMsg {0}", computeMsg);
    return computeMsg;
  }

  /**
   * Handler for task close event.
   * Stop this running task without noise.
   */
  final class TaskCloseHandler implements EventHandler<CloseEvent> {
    @Override
    public void onNext(final CloseEvent closeEvent) {
      if (terminated.getCount() == 0) {
        LOG.log(Level.INFO, "Tried to close task, but already terminated.");
      } else {
        LOG.log(Level.INFO, "Task closing {0}", closeEvent);
        isClosing.compareAndSet(false, true);
        terminated.countDown();
      }
    }
  }
}
