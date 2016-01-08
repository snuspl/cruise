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
import edu.snu.cay.dolphin.core.avro.IterationInfo;
import edu.snu.cay.dolphin.core.metric.avro.MetricsMessage;
import edu.snu.cay.dolphin.core.metric.avro.SrcType;
import edu.snu.cay.dolphin.core.optimizer.OptimizationOrchestrator;
import edu.snu.cay.dolphin.core.sync.ControllerTaskSyncRegister;
import edu.snu.cay.dolphin.core.sync.DriverSync;
import edu.snu.cay.dolphin.core.sync.SyncNetworkSetup;
import edu.snu.cay.dolphin.groupcomm.conf.GroupCommParameters;
import edu.snu.cay.dolphin.groupcomm.names.*;
import edu.snu.cay.dolphin.core.metric.MetricCodec;
import edu.snu.cay.dolphin.core.metric.MetricTracker;
import edu.snu.cay.dolphin.core.metric.MetricsCollectionService;
import edu.snu.cay.dolphin.core.metric.MetricTrackers;
import edu.snu.cay.dolphin.groupcomm.names.DataPreRunShuffle;
import edu.snu.cay.dolphin.parameters.StartTrace;
import edu.snu.cay.dolphin.scheduling.SchedulabilityAnalyzer;
import edu.snu.cay.services.dataloader.DataLoader;
import edu.snu.cay.services.em.avro.AvroElasticMemoryMessage;
import edu.snu.cay.services.em.avro.Result;
import edu.snu.cay.services.em.avro.ResultMsg;
import edu.snu.cay.services.em.avro.Type;
import edu.snu.cay.services.em.driver.ElasticMemoryConfiguration;
import edu.snu.cay.services.em.driver.api.EMDeleteExecutor;
import edu.snu.cay.services.em.driver.api.EMResourceRequestManager;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.impl.BaseCounterDataIdFactory;
import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.shuffle.common.ShuffleDescriptionImpl;
import edu.snu.cay.services.shuffle.driver.ShuffleDriver;
import edu.snu.cay.services.shuffle.driver.impl.StaticPushShuffleManager;
import edu.snu.cay.services.shuffle.strategy.KeyShuffleStrategy;
import edu.snu.cay.utils.trace.HTrace;
import edu.snu.cay.utils.trace.HTraceInfoCodec;
import edu.snu.cay.utils.trace.HTraceParameters;
import edu.snu.cay.utils.trace.HTraceUtils;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextMessage;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.task.*;
import org.apache.reef.evaluator.context.parameters.ContextIdentifier;
import org.apache.reef.io.data.loading.api.DataLoadingService;
import org.apache.reef.io.data.output.OutputService;
import org.apache.reef.io.network.group.api.driver.CommunicationGroupDriver;
import org.apache.reef.io.network.group.api.driver.GroupCommDriver;
import org.apache.reef.io.network.group.impl.config.BroadcastOperatorSpec;
import org.apache.reef.io.network.group.impl.config.GatherOperatorSpec;
import org.apache.reef.io.network.group.impl.config.ReduceOperatorSpec;
import org.apache.reef.io.network.group.impl.config.ScatterOperatorSpec;
import org.apache.reef.io.network.group.impl.driver.CommunicationGroupDriverImpl;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.task.Task;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.wake.time.event.StartTime;
import org.htrace.Sampler;
import org.htrace.Span;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.xml.bind.DatatypeConverter;
import java.util.*;
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

  public static final String DOLPHIN_PRE_RUN_SHUFFLE_PREFIX = "DOLPHIN_PRE_RUN_SHUFFLE_PREFIX";

  public static final String DOLPHIN_POST_RUN_SHUFFLE_PREFIX = "DOLPHIN_POST_RUN_SHUFFLE_PREFIX";

  private static final Logger LOG = Logger.getLogger(DolphinDriver.class.getName());

  /**
   * Sub-id for Control and Compute Tasks.
   * This object grants different IDs to each task
   * e.g. ComputeTask-0, ComputeTask-1, and so on.
   */
  private final AtomicInteger taskIdCounter = new AtomicInteger(0);

  /**
   * Fetcher object of the ID of the Context that goes under Controller Task.
   * The ID is used to distinguish the Context that represents the Controller Task
   * from Contexts that go under Compute Tasks.
   */
  private final CtrlTaskContextIdFetcher ctrlTaskContextIdFetcher;

  /**
   * Driver that manages Group Communication settings.
   */
  private final GroupCommDriver groupCommDriver;

  /**
   * Parameters used for creating Communication Groups.
   */
  private final GroupCommParameters groupCommParameters;

  /**
   * Driver that manages Shuffle settings.
   */
  private final ShuffleDriver shuffleDriver;

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
   * The output service.
   * Used to create an output service configuration for each context.
   */
  private final OutputService outputService;

  /**
   * Schedulability analyzer for the current Runtime.
   * Checks whether the number of evaluators configured by the data loading service can be gang scheduled.
   */
  private final SchedulabilityAnalyzer schedulabilityAnalyzer;

  private final DriverSync driverSync;

  private final TaskTracker taskTracker;

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
   * List of optional shuffle managers that manage pre-run shuffles.
   * If some stages do not have a pre-run shuffle operation, corresponding optional value is set to Optional.empty().
   */
  private final List<Optional<StaticPushShuffleManager>> preRunShuffleManagerList;

  /**
   * List of optional shuffle managers that manage post-run shuffles.
   * If some stages do not have a post-run shuffle operation, corresponding optional value is set to Optional.empty().
   */
  private final List<Optional<StaticPushShuffleManager>> postRunShuffleManagerList;

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

  private final HTraceInfoCodec hTraceInfoCodec;

  private final ObjectSerializableCodec<Long> codecLong = new ObjectSerializableCodec<>();
  private final UserParameters userParameters;

  private final TraceInfo jobTraceInfo;

  /**
   * Map of evaluator id to component id who requested the evaluator.
   */
  private final ConcurrentHashMap<String, String> evalToRequestorMap = new ConcurrentHashMap<>();

  /**
   * Manage callback of EM resource requests.
   */
  private final EMResourceRequestManager emResourceRequestManager;

  /**
   * Set of ActiveContext ids on state closing, used to determine whether a task is requested to be closed or not.
   */
  private final Set<String> closingContexts;

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
                        final GroupCommParameters groupCommParameters,
                        final ShuffleDriver shuffleDriver,
                        final DataLoadingService dataLoadingService,
                        final DataLoader dataLoader,
                        final OutputService outputService,
                        final SchedulabilityAnalyzer schedulabilityAnalyzer,
                        final OptimizationOrchestrator optimizationOrchestrator,
                        final DriverSync driverSync,
                        final TaskTracker taskTracker,
                        final ElasticMemoryConfiguration emConf,
                        final UserJobInfo userJobInfo,
                        final UserParameters userParameters,
                        final MetricCodec metricCodec,
                        final MetricsMessageCodec metricsMessageCodec,
                        final HTraceParameters traceParameters,
                        final EMResourceRequestManager emResourceRequestManager,
                        final HTraceInfoCodec hTraceInfoCodec,
                        final HTrace hTrace,
                        @Parameter(StartTrace.class) final boolean startTrace,
                        final CtrlTaskContextIdFetcher ctrlTaskContextIdFetcher) {
    hTrace.initialize();
    this.groupCommDriver = groupCommDriver;
    this.groupCommParameters = groupCommParameters;
    this.shuffleDriver = shuffleDriver;
    this.dataLoadingService = dataLoadingService;
    this.dataLoader = dataLoader;
    this.outputService = outputService;
    this.schedulabilityAnalyzer = schedulabilityAnalyzer;
    this.optimizationOrchestrator = optimizationOrchestrator;
    this.driverSync = driverSync;
    this.taskTracker = taskTracker;
    this.emConf = emConf;
    this.userJobInfo = userJobInfo;
    this.stageInfoList = userJobInfo.getStageInfoList();
    this.commGroupDriverList = new LinkedList<>();
    this.preRunShuffleManagerList = new LinkedList<>();
    this.postRunShuffleManagerList = new LinkedList<>();
    this.contextToStageSequence = new HashMap<>();
    this.userParameters = userParameters;
    this.metricCodec = metricCodec;
    this.metricsMessageCodec = metricsMessageCodec;
    this.traceParameters = traceParameters;
    this.emResourceRequestManager = emResourceRequestManager;
    this.closingContexts = Collections.synchronizedSet(new HashSet<String>());
    this.hTraceInfoCodec = hTraceInfoCodec;
    this.jobTraceInfo = startTrace ? TraceInfo.fromSpan(Trace.startSpan("job", Sampler.ALWAYS).getSpan()) : null;
    this.ctrlTaskContextIdFetcher = ctrlTaskContextIdFetcher;
    initializeDataOperators();
  }

  /**
   * Initialize the data operators.
   */
  private void initializeDataOperators() {
    if (!schedulabilityAnalyzer.isSchedulable()) {
      throw new RuntimeException("Schedulabiliy analysis shows that gang scheduling of " +
          dataLoadingService.getNumberOfPartitions() + " compute tasks and 1 controller task is not possible.");
    }

    int computeTaskIndex = stageInfoList.size();
    int sequence = 0;
    for (final StageInfo stageInfo : stageInfoList) {
      final int numTasks = dataLoadingService.getNumberOfPartitions() + 1;
      LOG.log(Level.INFO, "Initializing CommunicationGroupDriver with numTasks " + numTasks);
      final CommunicationGroupDriver commGroup = groupCommDriver.newCommunicationGroup(
          stageInfo.getCommGroupName(),
          groupCommParameters.getTopologyClass(),
          numTasks,
          groupCommParameters.getFanOut());
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

      final int numComputeTasks = numTasks - 1;

      // The names of shuffle senders and receivers should be defined first.
      if (stageInfo.isPreRunShuffleUsed() || stageInfo.isPostRunShuffleUsed()) {
        final List<String> computeTaskIdList = new ArrayList<>(numComputeTasks);

        for (int i = 0; i < numComputeTasks; i++) {
          computeTaskIdList.add(getCmpTaskId(computeTaskIndex));
          computeTaskIndex++;
        }

        if (stageInfo.isPreRunShuffleUsed()) {
          final StaticPushShuffleManager shuffleManager = registerPushShuffleManager(
              getPreRunShuffleName(sequence), sequence, computeTaskIdList, stageInfo.getPreRunShuffleKeyCodecClass(),
              stageInfo.getPreRunShuffleValueCodecClass());

          shuffleManager.setPushShuffleListener(new DolphinShuffleListener(sequence));
          preRunShuffleManagerList.add(Optional.of(shuffleManager));
        } else {
          preRunShuffleManagerList.add(Optional.<StaticPushShuffleManager>empty());
        }

        if (stageInfo.isPostRunShuffleUsed()) {
          final StaticPushShuffleManager shuffleManager = registerPushShuffleManager(
              getPostRunShuffleName(sequence), sequence, computeTaskIdList, stageInfo.getPostRunShuffleKeyCodecClass(),
              stageInfo.getPostRunShuffleValueCodecClass());

          shuffleManager.setPushShuffleListener(new DolphinShuffleListener(sequence));
          postRunShuffleManagerList.add(Optional.of(shuffleManager));
        } else {
          postRunShuffleManagerList.add(Optional.<StaticPushShuffleManager>empty());
        }
      } else {
        computeTaskIndex += numComputeTasks;
      }

      commGroupDriverList.add(commGroup);
      commGroup.finalise();
      sequence++;
    }
    taskIdCounter.set(sequence);
  }

  private StaticPushShuffleManager registerPushShuffleManager(
      final String shuffleName, final int sequence, final List<String> computeTaskIdList,
      final Class<? extends Codec> keyCodecClass, final Class<? extends Codec> valueCodecClass) {
    final StaticPushShuffleManager shuffleManager = shuffleDriver.registerShuffle(
        ShuffleDescriptionImpl.newBuilder(shuffleName)
            .setReceiverIdList(computeTaskIdList)
            .setSenderIdList(computeTaskIdList)
            .setKeyCodecClass(keyCodecClass)
            .setValueCodecClass(valueCodecClass)
            .setShuffleStrategyClass(KeyShuffleStrategy.class)
            .build());

    shuffleManager.setPushShuffleListener(new DolphinShuffleListener(sequence));
    return shuffleManager;
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
      LOG.log(Level.INFO, "Evaluator allocated {0}", allocatedEvaluator);
      if (dataLoader.isDataLoaderRequest()) {
        evalToRequestorMap.put(allocatedEvaluator.getId(), DataLoader.class.getName());
        dataLoader.handleDataLoadingEvalAlloc(allocatedEvaluator);
      } else if (emResourceRequestManager.bindCallback(allocatedEvaluator)) {
        evalToRequestorMap.put(allocatedEvaluator.getId(), ElasticMemory.class.getName());
        allocatedEvaluator.submitContextAndService(getContextConfiguration(), getServiceConfiguration());
      } else {
        LOG.log(Level.WARNING, "Unknown evaluator allocated. Ignore {0}", allocatedEvaluator);
        allocatedEvaluator.close();
      }
    }
  }

  final class EvaluatorFailedHandler implements EventHandler<FailedEvaluator> {

    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      final String evaluatorId = failedEvaluator.getId();
      final String requestorId = evalToRequestorMap.get(evaluatorId);
      if (requestorId == null) {
        LOG.warning("Failed to find a requestor for " + evaluatorId + ". Ignore " + failedEvaluator);
        return;
      }
      if (requestorId.equals(DataLoader.class.getName())) {
        dataLoader.handleDataLoadingEvalFailure(failedEvaluator);
      } else if (requestorId.equals(ElasticMemory.class.getName())) {
        // Failure handling for EM requested evaluators. We may use ElasticMemory.checkpoint to do this.
        // TODO #114: Implement Checkpoint
      } else {
        LOG.warning("Unknown failed evaluator. Ignore " + failedEvaluator);
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
    final Configuration metricCollectionServiceConf = MetricsCollectionService.getServiceConfiguration();
    final Configuration emServiceConf = emConf.getServiceConfigurationWithoutNameResolver();
    final Configuration traceConf = traceParameters.getConfiguration();
    final Configuration metricsMessageServiceConf = MetricsMessageSender.getServiceConfiguration();
    return Configurations.merge(
        userParameters.getServiceConf(), groupCommServiceConf, emServiceConf, metricCollectionServiceConf,
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
      final String requestorId = evalToRequestorMap.get(evaluatorId);
      if (requestorId == null) {
        LOG.warning("Failed to find a requestor for " + evaluatorId + ". Ignore " + activeContext);
        activeContext.close();
        return;
      }
      if (requestorId.equals(DataLoader.class.getName())) {
        if (!groupCommDriver.isConfigured(activeContext)) {
          final Configuration finalContextConf = getContextConfiguration();
          final Configuration finalServiceConf;
          if (dataLoadingService.isComputeContext(activeContext)) {
            LOG.log(Level.INFO, "Submitting GroupCommContext for ControllerTask to underlying context");
            ctrlTaskContextIdFetcher.setCtrlTaskContextId(getContextId(finalContextConf));

            // Add services for the GroupCommContext under ControllerTask
            finalServiceConf = getServiceConfiguration();
          } else {
            LOG.log(Level.INFO, "Submitting GroupCommContext for ComputeTask to underlying context");

            // Add services for the GroupCommContext under ComputeTask
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
        LOG.warning("Unknown evaluator. Ignore " + activeContext);
        activeContext.close();
      }
    }
  }

  final class FailedContextHandler implements EventHandler<FailedContext> {

    @Override
    public void onNext(final FailedContext failedContext) {
      LOG.log(Level.INFO, "{0} has failed.", failedContext);
      taskTracker.onFailedContext(failedContext);
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
              metricCodec.decode(metricsMessage.getMetrics().array()));

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
      optimizationOrchestrator.onRunningTask(runningTask);
      taskTracker.onRunningTask(runningTask);
    }
  }

  /**
   * When a certain task completes, the following task is submitted.
   */
  final class TaskCompletedHandler implements EventHandler<CompletedTask> {

    @Override
    public void onNext(final CompletedTask completedTask) {
      final String completedTaskId = completedTask.getId();
      LOG.log(Level.INFO, "{0} has completed.", completedTaskId);
      optimizationOrchestrator.onCompletedTask(completedTask);
      taskTracker.onCompletedTask(completedTask);

      final ActiveContext activeContext = completedTask.getActiveContext();
      final String contextId = activeContext.getId();

      if (closingContexts.remove(contextId)) {
        // close the context and release the container as requested
        activeContext.close();
        return;
      }

      final int nextSequence = contextToStageSequence.get(contextId) + 1;

      if (isCtrlTaskContextId(contextId)) {
        driverSync.onStageStop(stageInfoList.get(nextSequence - 1).getCommGroupName().getName());
      }

      if (nextSequence >= stageInfoList.size()) {
        completedTask.getActiveContext().close();
      } else {
        submitTask(activeContext, nextSequence);
      }
    }
  }

  final class TaskFailedHandler implements EventHandler<FailedTask> {

    @Override
    public void onNext(final FailedTask failedTask) {
      LOG.info(failedTask.getId() + " has failed.");
      optimizationOrchestrator.onFailedTask(failedTask);
      taskTracker.onFailedTask(failedTask);
    }
  }

  /**
   * Submit a task for an already running Stage.
   * @param activeContext the context to submit the task on
   * @param iterationInfo the iteration to start the task on
   */
  public void submitTask(final ActiveContext activeContext, final IterationInfo iterationInfo) {
    LOG.log(Level.INFO, "iterationInfo {0}", iterationInfo);
    final String commGroupName = iterationInfo.getCommGroupName().toString();
    int stageSequence = 0;
    for (final StageInfo stageInfo : stageInfoList) {
      if (commGroupName.equals(stageInfo.getCommGroupName().getName())) {
        submitTask(activeContext, stageSequence, iterationInfo.getIteration());
        return;
      }
      stageSequence++;
    }
    throw new RuntimeException("Unable to find stageSequence for " + iterationInfo);
  }

  private void submitTask(final ActiveContext activeContext, final int stageSequence) {
    submitTask(activeContext, stageSequence, 0);
  }

  private void submitTask(final ActiveContext activeContext, final int stageSequence, final int iteration) {
    contextToStageSequence.put(activeContext.getId(), stageSequence);
    final StageInfo stageInfo = stageInfoList.get(stageSequence);
    final CommunicationGroupDriver commGroup = commGroupDriverList.get(stageSequence);
    final Configuration partialTaskConf;

    final JavaConfigurationBuilder dolphinTaskConfBuilder = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(CommunicationGroup.class, stageInfo.getCommGroupName().getName())
        .bindNamedParameter(Iteration.class, Integer.toString(iteration));

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
    if (isCtrlTaskContextId(activeContext.getId())) {
      LOG.log(Level.INFO, "Submit ControllerTask");
      final String ctrlTaskId = getCtrlTaskId(stageSequence);
      try (final TraceScope controllerTaskTraceScope = Trace.startSpan(ctrlTaskId, jobTraceInfo)) {

        dolphinTaskConfBuilder
            .bindImplementation(DataIdFactory.class, BaseCounterDataIdFactory.class)
            .bindImplementation(UserControllerTask.class, stageInfo.getUserCtrlTaskClass())
            .bindNamedParameter(BaseCounterDataIdFactory.Base.class, String.valueOf(stageSequence));

        partialTaskConf = Configurations.merge(
            getCtrlTaskConf(ctrlTaskId, controllerTaskTraceScope.getSpan()),
            dolphinTaskConfBuilder.build(),
            SyncNetworkSetup.getControllerTaskConfiguration(),
            userParameters.getUserCtrlTaskConf());
      }

      driverSync.onStageStart(stageInfo.getCommGroupName().getName(), ctrlTaskId);
      // Case 2: Evaluator configured with a Group Communication context has been given,
      //         representing a Compute Task
      // We can now place a Compute Task on top of the contexts.
    } else {
      LOG.log(Level.INFO, "Submit ComputeTask");
      final int taskId = taskIdCounter.getAndIncrement();
      final String cmpTaskId = getCmpTaskId(taskId);
      try (final TraceScope computeTaskTraceScope = Trace.startSpan(cmpTaskId, jobTraceInfo)) {

        dolphinTaskConfBuilder
            .bindImplementation(UserComputeTask.class, stageInfo.getUserCmpTaskClass())
            .bindImplementation(DataIdFactory.class, BaseCounterDataIdFactory.class)
            .bindNamedParameter(BaseCounterDataIdFactory.Base.class, String.valueOf(taskId));

        partialTaskConf = Configurations.merge(
            getCmpTaskConf(cmpTaskId, computeTaskTraceScope.getSpan()),
            dolphinTaskConfBuilder.build(),
            userParameters.getUserCmpTaskConf(),
            getShuffleTaskConfiguration(stageSequence, cmpTaskId));
      }
    }

    // add the Task to our communication group
    commGroup.addTask(partialTaskConf);
    final Configuration finalTaskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
    activeContext.submitTask(finalTaskConf);
  }

  /**
   * EMDeleteExecutor implementation for Dolphin.
   * Dolphin uses group communication, so remove the task from group communication before closing it.
   * Assumes that EM delete is only called during {@code ControllerTaskSync.PAUSED} state.
   */
  final class TaskRemover implements EMDeleteExecutor {

    @Override
    public void execute(final String activeContextId, final EventHandler<AvroElasticMemoryMessage> callback) {
      final RunningTask runningTask = taskTracker.getRunningTask(activeContextId);
      if (runningTask == null) {
        // Given active context should have a runningTask in a normal case, because our job is paused.
        // Evaluator without corresponding runningTask implies error.
        LOG.log(Level.WARNING,
            "Trying to remove running task on active context {0}. Cannot find running task on it", activeContextId);
      } else {
        final int currentSequence = contextToStageSequence.get(runningTask.getActiveContext().getId());
        final CommunicationGroupDriverImpl commGroup
            = (CommunicationGroupDriverImpl) commGroupDriverList.get(currentSequence);
        commGroup.failTask(runningTask.getId());
        commGroup.removeTask(runningTask.getId());
        // Memo this context to release it after the task is completed
        closingContexts.add(activeContextId);
        runningTask.close();
        // TODO #205: Reconsider using of Avro message in EM's callback
        callback.onNext(AvroElasticMemoryMessage.newBuilder()
            .setType(Type.ResultMsg)
            .setResultMsg(ResultMsg.newBuilder().setResult(Result.SUCCESS).build())
            .setSrcId(activeContextId)
            .setDestId("")
            .build());
      }
    }
  }

  private Configuration getShuffleTaskConfiguration(final int stageSequence, final String computeTaskId) {
    final JavaConfigurationBuilder shuffleConfBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    if (stageInfoList.get(stageSequence).isPreRunShuffleUsed()) {
      shuffleConfBuilder.addConfiguration(
          preRunShuffleManagerList.get(stageSequence).get().getShuffleConfiguration(computeTaskId));
      shuffleConfBuilder.bindNamedParameter(DataPreRunShuffle.class, getPreRunShuffleName(stageSequence));
    }

    if (stageInfoList.get(stageSequence).isPostRunShuffleUsed()) {
      shuffleConfBuilder.addConfiguration(
          postRunShuffleManagerList.get(stageSequence).get().getShuffleConfiguration(computeTaskId));
      shuffleConfBuilder.bindNamedParameter(DataPostRunShuffle.class, getPostRunShuffleName(stageSequence));
    }

    return shuffleConfBuilder.build();
  }

  private boolean isCtrlTaskContextId(final String id) {
    final Optional<String> ctrlTaskContextId = ctrlTaskContextIdFetcher.getCtrlTaskContextId();
    if (!ctrlTaskContextId.isPresent()) {
      return false;
    } else {
      return ctrlTaskContextId.get().equals(id);
    }
  }

  private String getCtrlTaskId(final int sequence) {
    return ControllerTask.TASK_ID_PREFIX + "-" + sequence;
  }

  private String getCmpTaskId(final int sequence) {
    return ComputeTask.TASK_ID_PREFIX + "-" + sequence;
  }

  private Configuration getCtrlTaskConf(final String identifier, @Nullable final Span traceSpan) {
    return getBaseTaskConfModule(identifier, ControllerTask.class, traceSpan)
        .set(TaskConfiguration.ON_TASK_STARTED, ControllerTaskSyncRegister.RegisterTaskHandler.class)
        .set(TaskConfiguration.ON_TASK_STOP, ControllerTaskSyncRegister.UnregisterTaskHandler.class)
        .build();
  }

  private Configuration getCmpTaskConf(final String identifier, @Nullable final Span traceSpan) {
    return getBaseTaskConfModule(identifier, ComputeTask.class, traceSpan)
        .set(TaskConfiguration.ON_CLOSE, ComputeTask.TaskCloseHandler.class)
        .build();
  }

  private ConfigurationModule getBaseTaskConfModule(final String identifier,
                                                    final Class<? extends Task> taskClass,
                                                    @Nullable final Span traceSpan) {
    if (traceSpan == null) {

      return TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, identifier)
          .set(TaskConfiguration.TASK, taskClass);
    } else {

      return TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, identifier)
          .set(TaskConfiguration.TASK, taskClass)
          .set(TaskConfiguration.MEMENTO, DatatypeConverter.printBase64Binary(
              hTraceInfoCodec.encode(
                  HTraceUtils.toAvro(TraceInfo.fromSpan(traceSpan)))));
    }
  }

  private String getPreRunShuffleName(final int stageSequence) {
    return DOLPHIN_PRE_RUN_SHUFFLE_PREFIX + stageSequence;
  }

  private String getPostRunShuffleName(final int stageSequence) {
    return DOLPHIN_POST_RUN_SHUFFLE_PREFIX + stageSequence;
  }
}
