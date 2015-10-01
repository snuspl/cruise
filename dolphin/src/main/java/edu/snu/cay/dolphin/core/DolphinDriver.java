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

import edu.snu.cay.dolphin.core.metric.MetricsMessageCodec;
import edu.snu.cay.dolphin.core.metric.MetricsMessageSender;
import edu.snu.cay.dolphin.core.metric.avro.MetricsMessage;
import edu.snu.cay.dolphin.core.metric.avro.SrcType;
import edu.snu.cay.dolphin.core.optimizer.OptimizationOrchestrator;
import edu.snu.cay.dolphin.groupcomm.names.*;
import edu.snu.cay.dolphin.core.metric.MetricCodec;
import edu.snu.cay.dolphin.core.metric.MetricTracker;
import edu.snu.cay.dolphin.core.metric.MetricsCollectionService;
import edu.snu.cay.dolphin.core.metric.MetricTrackers;
import edu.snu.cay.dolphin.scheduling.SchedulabilityAnalyzer;
import edu.snu.cay.services.dataloader.DataLoader;
import edu.snu.cay.services.em.driver.ElasticMemoryConfiguration;
import edu.snu.cay.services.em.driver.api.EMResourceRequestManager;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.impl.BaseCounterDataIdFactory;
import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.utils.trace.HTraceParameters;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextMessage;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.evaluator.context.parameters.ContextIdentifier;
import org.apache.reef.io.data.loading.api.DataLoadingService;
import org.apache.reef.io.data.output.OutputService;
import org.apache.reef.io.network.group.api.driver.CommunicationGroupDriver;
import org.apache.reef.io.network.group.api.driver.GroupCommDriver;
import org.apache.reef.io.network.group.impl.config.BroadcastOperatorSpec;
import org.apache.reef.io.network.group.impl.config.GatherOperatorSpec;
import org.apache.reef.io.network.group.impl.config.ReduceOperatorSpec;
import org.apache.reef.io.network.group.impl.config.ScatterOperatorSpec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver code for Dolphin applications.
 * This class is appropriate for setting up event handlers as well as configuring
 * Broadcast (and/or Scatter) and Reduce (and/or Gather) operations for Group Communication.
 */
@Unit
public final class DolphinDriver {
  private static final Logger LOG = Logger.getLogger(DolphinDriver.class.getName());

  /**
   * Sub-id for Control and Compute Tasks.
   * This object grants different IDs to each task
   * e.g. ComputeTask-0, ComputeTask-1, and so on.
   */
  private final AtomicInteger taskIdCounter = new AtomicInteger(0);

  /**
   * ID of the Context that goes under Controller Task.
   * This string is used to distinguish the Context that represents the Controller Task
   * from Contexts that go under Compute Tasks.
   */
  private String ctrlTaskContextId;

  /**
   * Driver that manages Group Communication settings.
   */
  private final GroupCommDriver groupCommDriver;

  /**
   * Accessor for data loading service.
   * Can check whether a evaluator is configured with the service or not.
   */
  private final DataLoadingService dataLoadingService;

  /**
   * Customized REEF DataLoader.
   * Sends evaluator allocation requests for data loading.
   */
  private final DataLoader dataLoader;

  /**
   * Elastic Memory to add, delete evaluators and move data.
   */
  private final ElasticMemory elasticMemory;

  /**
   * The output service.
   * Used to create an output service configuration for each context.
   */
  private final OutputService outputService;

  /**
   * Schedulability analyzer for the current Runtime.
   * Checks whether the number of evaluators configured by the data loading service can be gang scheduled.
   */
  private final SchedulabilityAnalyzer schedulabilityAnalyzer;

  /**
   * Manager of the configuration of Elastic Memory service.
   */
  private final ElasticMemoryConfiguration emConf;

  /**
   * Job to execute.
   */
  private final UserJobInfo userJobInfo;

  /**
   * List of stages composing the job to execute.
   */
  private final List<StageInfo> stageInfoList;

  /**
   * List of communication group drivers.
   * Each group driver is matched to the corresponding stage.
   */
  private final List<CommunicationGroupDriver> commGroupDriverList;

  /**
   * Map to record which stage is being executed by each evaluator which is identified by context id.
   */
  private final Map<String, Integer> contextToStageSequence;

  private final OptimizationOrchestrator optimizationOrchestrator;

  /**
   * Codec for metrics.
   */
  private final MetricCodec metricCodec;

  /**
   * Codec for metrics messages.
   */
  private final MetricsMessageCodec metricsMessageCodec;

  private final HTraceParameters traceParameters;

  private final ObjectSerializableCodec<Long> codecLong = new ObjectSerializableCodec<>();
  private final UserParameters userParameters;

  /**
   * Map of evaluator id to component id who requested the evaluator.
   */
  private final ConcurrentHashMap<String, String> idMap = new ConcurrentHashMap<>();

  /**
   * Manage callback of EM resource requests.
   */
  private final EMResourceRequestManager emResourceRequestManager;


  /**
   * Constructor for the Driver of a Dolphin job.
   * Store various objects as well as configuring Group Communication with
   * Broadcast and Reduce operations to use.
   *
   * This class is instantiated by TANG.
   *
   * @param groupCommDriver manager for Group Communication configurations
   * @param dataLoadingService manager for Data Loading configurations
   * @param outputService
   * @param schedulabilityAnalyzer
   * @param emConf manager for Elastic Memory configurations
   * @param userJobInfo
   * @param userParameters
   * @param metricCodec
   */
  @Inject
  private DolphinDriver(final GroupCommDriver groupCommDriver,
                        final DataLoadingService dataLoadingService,
                        final DataLoader dataLoader,
                        final ElasticMemory elasticMemory,
                        final OutputService outputService,
                        final SchedulabilityAnalyzer schedulabilityAnalyzer,
                        final OptimizationOrchestrator optimizationOrchestrator,
                        final ElasticMemoryConfiguration emConf,
                        final UserJobInfo userJobInfo,
                        final UserParameters userParameters,
                        final MetricCodec metricCodec,
                        final MetricsMessageCodec metricsMessageCodec,
                        final HTraceParameters traceParameters,
                        final EMResourceRequestManager emResourceRequestManager) {
    this.groupCommDriver = groupCommDriver;
    this.dataLoadingService = dataLoadingService;
    this.dataLoader = dataLoader;
    this.elasticMemory = elasticMemory;
    this.outputService = outputService;
    this.schedulabilityAnalyzer = schedulabilityAnalyzer;
    this.optimizationOrchestrator = optimizationOrchestrator;
    this.emConf = emConf;
    this.userJobInfo = userJobInfo;
    this.stageInfoList = userJobInfo.getStageInfoList();
    this.commGroupDriverList = new LinkedList<>();
    this.contextToStageSequence = new HashMap<>();
    this.userParameters = userParameters;
    this.metricCodec = metricCodec;
    this.metricsMessageCodec = metricsMessageCodec;
    this.traceParameters = traceParameters;
    this.emResourceRequestManager = emResourceRequestManager;
    initializeCommDriver();
  }

  /**
   * Initialize the group communication driver.
   */
  private void initializeCommDriver() {
    if (!schedulabilityAnalyzer.isSchedulable()) {
      throw new RuntimeException("Schedulabiliy analysis shows that gang scheduling of " +
          dataLoadingService.getNumberOfPartitions() + " compute tasks and 1 controller task is not possible.");
    }

    int sequence = 0;
    for (final StageInfo stageInfo : stageInfoList) {
      final int numTasks = dataLoadingService.getNumberOfPartitions() + 1;
      LOG.log(Level.INFO, "Initializing CommunicationGroupDriver with numTasks " + numTasks);
      final CommunicationGroupDriver commGroup = groupCommDriver.newCommunicationGroup(
          stageInfo.getCommGroupName(), numTasks);
      commGroup.addBroadcast(CtrlMsgBroadcast.class,
          BroadcastOperatorSpec.newBuilder()
              .setSenderId(getCtrlTaskId(sequence))
              .setDataCodecClass(SerializableCodec.class)
              .build());
      if (stageInfo.isBroadcastUsed()) {
        commGroup.addBroadcast(DataBroadcast.class,
            BroadcastOperatorSpec.newBuilder()
                .setSenderId(getCtrlTaskId(sequence))
                .setDataCodecClass(stageInfo.getBroadcastCodecClass())
                .build());
      }
      if (stageInfo.isScatterUsed()) {
        commGroup.addScatter(DataScatter.class,
            ScatterOperatorSpec.newBuilder()
                .setSenderId(getCtrlTaskId(sequence))
                .setDataCodecClass(stageInfo.getScatterCodecClass())
                .build());
      }
      if (stageInfo.isReduceUsed()) {
        commGroup.addReduce(DataReduce.class,
            ReduceOperatorSpec.newBuilder()
                .setReceiverId(getCtrlTaskId(sequence))
                .setDataCodecClass(stageInfo.getReduceCodecClass())
                .setReduceFunctionClass(stageInfo.getReduceFunctionClass())
                .build());
      }
      if (stageInfo.isGatherUsed()) {
        commGroup.addGather(DataGather.class,
            GatherOperatorSpec.newBuilder()
                .setReceiverId(getCtrlTaskId(sequence))
                .setDataCodecClass(stageInfo.getGatherCodecClass())
                .build());
      }
      commGroupDriverList.add(commGroup);
      commGroup.finalise();
      sequence++;
    }
    taskIdCounter.set(sequence);
  }

  final class StartHandler implements EventHandler<StartTime> {

    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "StartTime: {0}", startTime);
      dataLoader.releaseResourceRequestGate();
    }
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      if (dataLoader.isDataLoaderRequest()) {
        idMap.put(allocatedEvaluator.getId(), DataLoader.class.getName());
        dataLoader.handleDataLoadingEvalAlloc(allocatedEvaluator);
      } else if (emResourceRequestManager.isEMRequest(allocatedEvaluator)) {
        idMap.put(allocatedEvaluator.getId(), ElasticMemory.class.getName());
        allocatedEvaluator.submitContextAndService(getContextConfiguration(), getServiceConfiguration());
      } else {
        LOG.warning("Unknown evaluator allocated. Ignore " + allocatedEvaluator.toString());
      }
    }
  }

  final class EvaluatorFailedHandler implements EventHandler<FailedEvaluator> {

    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      final String evaluatorId = failedEvaluator.getId();
      final String requestorId = idMap.get(evaluatorId);
      if (requestorId == null) {
        LOG.warning("Failed to find a requestor for " + evaluatorId + ". Ignore " + failedEvaluator.toString());
        return;
      }
      if (requestorId.equals(DataLoader.class.getName())) {
        dataLoader.handleDataLoadingEvalFailure(failedEvaluator);
      } else if (requestorId.equals(ElasticMemory.class.getName())) {
        // Failure handling for EM requested evaluators. We may use ElasticMemory.checkpoint to do this.
      } else {
        LOG.warning("Unknown failed evaluator. Ignore " + failedEvaluator.toString());
      }
    }
  }

  /**
   * Gives context configuration submitted on
   * both DataLoader requested evaluators and ElasticMemory requested evaluators.
   */
  private Configuration getContextConfiguration() {
    final Configuration groupCommContextConf = groupCommDriver.getContextConfiguration();
    final Configuration emContextConf = emConf.getContextConfiguration();
    final Configuration metricsMessageContextConf = MetricsMessageSender.getContextConfiguration();
    return Configurations.merge(groupCommContextConf, emContextConf, metricsMessageContextConf);
  }

  /**
   * Gives service configuration submitted on
   * both DataLoader requested evaluators and ElasticMemory requested evaluators.
   */
  private Configuration getServiceConfiguration() {
    final Configuration groupCommServiceConf = groupCommDriver.getServiceConfiguration();
    final Configuration outputServiceConf = outputService.getServiceConfiguration();
    final Configuration metricTrackerServiceConf = MetricsCollectionService.getServiceConfiguration();
    final Configuration emServiceConf = emConf.getServiceConfigurationWithoutNameResolver();
    final Configuration traceConf = traceParameters.getConfiguration();
    final Configuration metricsMessageServiceConf = MetricsMessageSender.getServiceConfiguration();
    return Configurations.merge(
        userParameters.getServiceConf(), groupCommServiceConf, emServiceConf, metricTrackerServiceConf,
        outputServiceConf, traceConf, metricsMessageServiceConf);
  }

  final class ActiveContextHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(final ActiveContext activeContext) {

      // Evaluator configured with a Data Loading context has been given
      // We need to add a Group Communication context above this context.
      //
      // It would be better if the two services could go into the same context, but
      // the Data Loading API is currently constructed to add its own context before
      // allowing any other ones.
      final String evaluatorId = activeContext.getEvaluatorId();
      final String requestorId = idMap.get(evaluatorId);
      if (requestorId == null) {
        LOG.warning("Failed to find a requestor for " + evaluatorId + ". Ignore " + activeContext.toString());
        return;
      }
      if (requestorId.equals(DataLoader.class.getName())) {
        if (!groupCommDriver.isConfigured(activeContext)) {
          final Configuration finalContextConf = getContextConfiguration();
          final Configuration finalServiceConf;
          if (dataLoadingService.isComputeContext(activeContext)) {
            LOG.log(Level.INFO, "Submitting GroupCommContext for ControllerTask to underlying context");
            ctrlTaskContextId = getContextId(finalContextConf);

            // Add the Elastic Memory service, the Output service,
            // the Metric Collection service, and the Group Communication service
            finalServiceConf = getServiceConfiguration();
          } else {
            LOG.log(Level.INFO, "Submitting GroupCommContext for ComputeTask to underlying context");

            // Add the Data Parse service, the Elastic Memory service,
            // the Output service, the Metric Collection service, and the Group Communication service
            final Configuration dataParseConf = DataParseService.getServiceConfiguration(userJobInfo.getDataParser());
            finalServiceConf = Configurations.merge(getServiceConfiguration(), dataParseConf);
          }
          activeContext.submitContextAndService(finalContextConf, finalServiceConf);
        } else {
          submitTask(activeContext, 0);
        }
      } else if (requestorId.equals(ElasticMemory.class.getName())) {
        emResourceRequestManager.onCompleted(activeContext);
      } else {
        LOG.warning("Unknown evaluator. Ignore " + activeContext.toString());
      }
    }
  }

  /**
   * Return the ID of the given Context.
   */
  private String getContextId(final Configuration contextConf) {
    try {
      final Injector injector = Tang.Factory.getTang().newInjector(contextConf);
      return injector.getNamedInstance(ContextIdentifier.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("Unable to inject context identifier from context conf", e);
    }
  }

  /**
   * Receives metrics from context.
   * TODO #172: Use NetworkConnectionService to replace the heartbeat
   */
  final class ContextMessageHandler implements EventHandler<ContextMessage> {

    @Override
    public void onNext(final ContextMessage message) {

      if (message.getMessageSourceID().equals(MetricsCollectionService.class.getName())) {

        LOG.info("Metrics are gathered from " + message.getId());
        final MetricsMessage metricsMessage = metricsMessageCodec.decode(message.get());

        final SrcType srcType = metricsMessage.getSrcType();
        if (SrcType.Compute.equals(srcType)) {

          optimizationOrchestrator.receiveComputeMetrics(message.getId(),
              metricsMessage.getIterationInfo().getCommGroupName().toString(),
              metricsMessage.getIterationInfo().getIteration(),
              metricCodec.decode(metricsMessage.getMetrics().array()),
              getDataInfoFromAvro(metricsMessage.getComputeMsg().getDataInfos()));

        } else if (SrcType.Controller.equals(srcType)) {

          optimizationOrchestrator.receiveControllerMetrics(message.getId(),
              metricsMessage.getIterationInfo().getCommGroupName().toString(),
              metricsMessage.getIterationInfo().getIteration(),
              metricCodec.decode(metricsMessage.getMetrics().array()),
              metricsMessage.getControllerMsg().getNumComputeTasks());

        } else {
          throw new RuntimeException("Unknown SrcType " + srcType);
        }
      }
    }

    private List<DataInfo> getDataInfoFromAvro(
        final List<edu.snu.cay.dolphin.core.metric.avro.DataInfo> avroDataInfos) {
      final List<DataInfo> dataInfos = new ArrayList<>(avroDataInfos.size());
      for (final edu.snu.cay.dolphin.core.metric.avro.DataInfo avroDataInfo : avroDataInfos) {
        dataInfos.add(new DataInfoImpl(avroDataInfo.getDataType().toString(), avroDataInfo.getNumUnits()));
      }
      return dataInfos;
    }
  }

  final class TaskRunningHandler implements EventHandler<RunningTask> {

    @Override
    public void onNext(final RunningTask runningTask) {
      LOG.info(runningTask.getId() + " has started.");
    }
  }

  /**
   * When a certain task completes, the following task is submitted.
   */
  final class TaskCompletedHandler implements EventHandler<CompletedTask> {

    @Override
    public void onNext(final CompletedTask completedTask) {
      LOG.info(completedTask.getId() + " has completed.");

      final ActiveContext activeContext = completedTask.getActiveContext();
      final String contextId = activeContext.getId();
      final int nextSequence = contextToStageSequence.get(contextId) + 1;
      if (nextSequence >= stageInfoList.size()) {
        completedTask.getActiveContext().close();
        return;
      } else {
        submitTask(activeContext, nextSequence);
      }
    }
  }

  final class TaskFailedHandler implements EventHandler<FailedTask> {

    @Override
    public void onNext(final FailedTask failedTask) {
      LOG.info(failedTask.getId() + " has failed.");
    }
  }

  /**
   * Execute the task of the given stage.
   * @param activeContext
   * @param stageSequence
   */
  private void submitTask(final ActiveContext activeContext, final int stageSequence) {
    contextToStageSequence.put(activeContext.getId(), stageSequence);
    final StageInfo stageInfo = stageInfoList.get(stageSequence);
    final CommunicationGroupDriver commGroup = commGroupDriverList.get(stageSequence);
    final Configuration partialTaskConf;

    final JavaConfigurationBuilder dolphinTaskConfBuilder = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(CommunicationGroup.class, stageInfo.getCommGroupName().getName());

    //Set metric trackers
    if (stageInfo.getMetricTrackerClassSet() != null) {
      for (final Class<? extends MetricTracker> metricTrackerClass
          : stageInfo.getMetricTrackerClassSet()) {
        dolphinTaskConfBuilder.bindSetEntry(MetricTrackers.class, metricTrackerClass);
      }
    }

    // Case 1: Evaluator configured with a Group Communication context has been given,
    //         representing a Controller Task
    // We can now place a Controller Task on top of the contexts.
    if (isCtrlTaskId(activeContext.getId())) {
      LOG.log(Level.INFO, "Submit ControllerTask");

      dolphinTaskConfBuilder
          .bindImplementation(DataIdFactory.class, BaseCounterDataIdFactory.class)
          .bindImplementation(UserControllerTask.class, stageInfo.getUserCtrlTaskClass())
          .bindNamedParameter(BaseCounterDataIdFactory.Base.class, String.valueOf(stageSequence));
      partialTaskConf = Configurations.merge(
          TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, getCtrlTaskId(stageSequence))
              .set(TaskConfiguration.TASK, ControllerTask.class)
              .build(),
          dolphinTaskConfBuilder.build(),
          userParameters.getUserCtrlTaskConf());

      // Case 2: Evaluator configured with a Group Communication context has been given,
      //         representing a Compute Task
      // We can now place a Compute Task on top of the contexts.
    } else {
      LOG.log(Level.INFO, "Submit ComputeTask");

      final int taskId = taskIdCounter.getAndIncrement();
      dolphinTaskConfBuilder
          .bindImplementation(UserComputeTask.class, stageInfo.getUserCmpTaskClass())
          .bindImplementation(DataIdFactory.class, BaseCounterDataIdFactory.class)
          .bindNamedParameter(BaseCounterDataIdFactory.Base.class, String.valueOf(taskId));
      partialTaskConf = Configurations.merge(
          TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, getCmpTaskId(taskId))
              .set(TaskConfiguration.TASK, ComputeTask.class)
              .build(),
          dolphinTaskConfBuilder.build(),
          userParameters.getUserCmpTaskConf());
    }

    // add the Task to our communication group
    commGroup.addTask(partialTaskConf);
    final Configuration finalTaskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
    activeContext.submitTask(finalTaskConf);
  }

  private boolean isCtrlTaskId(final String id) {
    if (ctrlTaskContextId == null) {
      return false;
    } else {
      return ctrlTaskContextId.equals(id);
    }
  }

  private String getCtrlTaskId(final int sequence) {
    return ControllerTask.TASK_ID_PREFIX + "-" + sequence;
  }

  private String getCmpTaskId(final int sequence) {
    return ComputeTask.TASK_ID_PREFIX + "-" + sequence;
  }
}
