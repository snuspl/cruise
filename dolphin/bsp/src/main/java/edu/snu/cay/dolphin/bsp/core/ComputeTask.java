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
import edu.snu.cay.dolphin.bsp.core.metric.MetricsMessageSender;
import edu.snu.cay.dolphin.bsp.groupcomm.interfaces.*;
import edu.snu.cay.dolphin.bsp.groupcomm.names.*;
import edu.snu.cay.dolphin.core.avro.IterationInfo;
import edu.snu.cay.dolphin.core.metric.avro.ComputeMsg;
import edu.snu.cay.dolphin.core.metric.avro.DataInfo;

import static edu.snu.cay.dolphin.bsp.core.DolphinMetricKeys.COMPUTE_TASK_EXCHANGE_PRE_RUN_SHUFFLE_DATA_START;
import static edu.snu.cay.dolphin.bsp.core.DolphinMetricKeys.COMPUTE_TASK_EXCHANGE_PRE_RUN_SHUFFLE_DATA_END;
import static edu.snu.cay.dolphin.bsp.core.DolphinMetricKeys.COMPUTE_TASK_EXCHANGE_POST_RUN_SHUFFLE_DATA_START;
import static edu.snu.cay.dolphin.bsp.core.DolphinMetricKeys.COMPUTE_TASK_EXCHANGE_POST_RUN_SHUFFLE_DATA_END;
import static edu.snu.cay.dolphin.bsp.core.DolphinMetricKeys.COMPUTE_TASK_RECEIVE_DATA_START;
import static edu.snu.cay.dolphin.bsp.core.DolphinMetricKeys.COMPUTE_TASK_RECEIVE_DATA_END;
import static edu.snu.cay.dolphin.bsp.core.DolphinMetricKeys.COMPUTE_TASK_SEND_DATA_START;
import static edu.snu.cay.dolphin.bsp.core.DolphinMetricKeys.COMPUTE_TASK_SEND_DATA_END;
import static edu.snu.cay.dolphin.bsp.core.DolphinMetricKeys.COMPUTE_TASK_USER_COMPUTE_TASK_START;
import static edu.snu.cay.dolphin.bsp.core.DolphinMetricKeys.COMPUTE_TASK_USER_COMPUTE_TASK_END;

import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.shuffle.evaluator.ShuffleProvider;
import edu.snu.cay.services.shuffle.evaluator.operator.PushShuffleReceiver;
import edu.snu.cay.services.shuffle.evaluator.operator.PushShuffleSender;
import edu.snu.cay.utils.trace.HTraceInfoCodec;
import edu.snu.cay.utils.trace.HTraceUtils;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.Tuple;
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
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
public final class ComputeTask implements Task {
  public static final String TASK_ID_PREFIX = "CmpTask";
  private static final Logger LOG = Logger.getLogger(ComputeTask.class.getName());

  private final ShuffleProvider shuffleProvider;
  private final String taskId;
  private final MemoryStore<Long> memoryStore;
  private final UserComputeTask userComputeTask;
  private final CommunicationGroupClient commGroup;
  private final Broadcast.Receiver<CtrlMessage> ctrlMessageBroadcast;
  private final PushShuffleSender preRunShuffleSender;
  private final PushShuffleSender postRunShuffleSender;
  private final MetricsCollector metricsCollector;
  private final Set<MetricTracker> metricTrackerSet;
  private final InsertableMetricTracker insertableMetricTracker;
  private final MetricsMessageSender metricsMessageSender;
  private final HTraceInfoCodec hTraceInfoCodec;
  private final UserTaskTrace userTaskTrace;

  private final Queue<Tuple> receivedTupleQueue;
  private final CountDownLatch terminated = new CountDownLatch(1);
  private final AtomicBoolean isClosing = new AtomicBoolean(false);
  private int iteration;

  @Inject
  public ComputeTask(final GroupCommClient groupCommClient,
                     final ShuffleProvider shuffleProvider,
                     @Parameter(DataPreRunShuffle.class) final String preRunShuffleName,
                     @Parameter(DataPostRunShuffle.class) final String postRunShuffleName,
                     final MemoryStore<Long> memoryStore,
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
    this.shuffleProvider = shuffleProvider;
    this.memoryStore = memoryStore;
    this.userComputeTask = userComputeTask;
    this.taskId = taskId;
    this.commGroup =
        groupCommClient.getCommunicationGroup((Class<? extends Name<String>>) Class.forName(commGroupName));
    this.iteration = startIteration;
    this.ctrlMessageBroadcast = commGroup.getBroadcastReceiver(CtrlMsgBroadcast.class);
    this.receivedTupleQueue = new ConcurrentLinkedQueue<>();

    // TODO #223: Use ShuffleProvider's method to check the shuffle is used or not
    if (preRunShuffleName.startsWith(DolphinDriver.DOLPHIN_PRE_RUN_SHUFFLE_PREFIX)) {
      this.preRunShuffleSender = setReceiverAndGetShuffleSender(preRunShuffleName);
    } else {
      this.preRunShuffleSender = null;
    }

    // TODO #223: Use ShuffleProvider's method to check the shuffle is used or not
    if (postRunShuffleName.startsWith(DolphinDriver.DOLPHIN_POST_RUN_SHUFFLE_PREFIX)) {
      this.postRunShuffleSender = setReceiverAndGetShuffleSender(postRunShuffleName);
    } else {
      this.postRunShuffleSender = null;
    }

    this.metricsCollector = metricsCollector;
    this.metricTrackerSet = metricTrackerSet;
    this.insertableMetricTracker = insertableMetricTracker;
    this.metricsMessageSender = metricsMessageSender;
    this.hTraceInfoCodec = hTraceInfoCodec;
    this.userTaskTrace = userTaskTrace;
  }

  private PushShuffleSender setReceiverAndGetShuffleSender(final String shuffleName) {
    final PushShuffleSender sender  = shuffleProvider.getShuffle(shuffleName).getSender();
    final PushShuffleReceiver shuffleReceiver = shuffleProvider.getShuffle(shuffleName).getReceiver();
    shuffleReceiver.registerDataListener(new ShuffleDataReceiver(receivedTupleQueue));
    return sender;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, String.format("%s starting...", taskId));
    final TraceInfo taskTraceInfo = memento == null ? null :
        HTraceUtils.fromAvro(hTraceInfoCodec.decode(memento));

    initializeGroupCommOperators();
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
              exchangePreRunShuffleData();
              runUserComputeTask();
              exchangePostRunShuffleData();
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

    shuffleProvider.close();
    return null;
  }

  private void initializeGroupCommOperators() {
    if (userComputeTask.isBroadcastUsed()) {
      commGroup.getBroadcastReceiver(DataBroadcast.class);
    }
    if (userComputeTask.isScatterUsed()) {
      commGroup.getScatterReceiver(DataScatter.class);
    }
    if (userComputeTask.isGatherUsed()) {
      commGroup.getGatherSender(DataGather.class);
    }
    if (userComputeTask.isReduceUsed()) {
      commGroup.getReduceSender(DataReduce.class);
    }
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

  private void exchangePreRunShuffleData() throws MetricException {
    insertableMetricTracker.put(COMPUTE_TASK_EXCHANGE_PRE_RUN_SHUFFLE_DATA_START, System.currentTimeMillis());
    if (userComputeTask.isPreRunShuffleUsed()) {
      preRunShuffleSender.sendTuple(((DataPreRunShuffleOperator) userComputeTask).sendPreRunShuffleData(iteration));
      preRunShuffleSender.complete();
      ((DataPreRunShuffleOperator)userComputeTask)
          .receivePreRunShuffleData(iteration, getAndClearReceivedTupleList());
    }
    insertableMetricTracker.put(COMPUTE_TASK_EXCHANGE_PRE_RUN_SHUFFLE_DATA_END, System.currentTimeMillis());
  }

  private void exchangePostRunShuffleData() throws MetricException {
    insertableMetricTracker.put(COMPUTE_TASK_EXCHANGE_POST_RUN_SHUFFLE_DATA_START, System.currentTimeMillis());
    if (userComputeTask.isPostRunShuffleUsed()) {
      postRunShuffleSender.sendTuple(((DataPostRunShuffleOperator) userComputeTask).sendPostRunShuffleData(iteration));
      postRunShuffleSender.complete();
      ((DataPostRunShuffleOperator)userComputeTask)
          .receivePostRunShuffleData(iteration, getAndClearReceivedTupleList());
    }
    insertableMetricTracker.put(COMPUTE_TASK_EXCHANGE_POST_RUN_SHUFFLE_DATA_END, System.currentTimeMillis());
  }

  private List<Tuple> getAndClearReceivedTupleList() {
    final List<Tuple> tupleList = new ArrayList<>(receivedTupleQueue);
    receivedTupleQueue.clear();
    return tupleList;
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

    LOG.log(Level.FINE, "iterationInfo {0}", iterationInfo);
    return iterationInfo;
  }

  private ComputeMsg getComputeMsg() {
    final List<DataInfo> dataInfos = new ArrayList<>();
    for (final String dataType : memoryStore.getDataTypes()) {
      dataInfos.add(
          DataInfo.newBuilder()
              .setDataType(dataType)
              .setNumUnits(memoryStore.getNumUnits(dataType))
              .build());
    }

    final ComputeMsg computeMsg = ComputeMsg.newBuilder()
        .setDataInfos(dataInfos)
        .build();
    LOG.log(Level.FINE, "computeMsg {0}", computeMsg);
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
