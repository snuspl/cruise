/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.dolphin.async;

import edu.snu.cay.common.metric.MetricsCollectionServiceConf;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.AsyncDolphinLauncher.*;
import edu.snu.cay.dolphin.async.metric.MetricManager;
import edu.snu.cay.dolphin.async.metric.WorkerMetricsMsgCodec;
import edu.snu.cay.dolphin.async.metric.WorkerMetricsMsgSender;
import edu.snu.cay.dolphin.async.optimizer.*;
import edu.snu.cay.dolphin.async.optimizer.parameters.OptimizationIntervalMs;
import edu.snu.cay.common.aggregation.driver.AggregationManager;
import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.common.parameters.AddedEval;
import edu.snu.cay.services.em.common.parameters.MemoryStoreId;
import edu.snu.cay.services.em.common.parameters.ConsistencyPreserved;
import edu.snu.cay.services.em.common.parameters.RangeSupport;
import edu.snu.cay.services.em.driver.EMWrapper;
import edu.snu.cay.services.em.driver.api.EMDeleteExecutor;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.impl.RoundRobinDataIdFactory;
import edu.snu.cay.services.em.ns.parameters.EMIdentifier;
import edu.snu.cay.services.evalmanager.api.EvaluatorManager;
import edu.snu.cay.services.ps.common.parameters.NumServers;
import edu.snu.cay.services.ps.driver.impl.ClockManager;
import edu.snu.cay.services.ps.driver.impl.PSDriver;
import edu.snu.cay.services.ps.driver.impl.EMRoutingTableManager;
import edu.snu.cay.services.ps.metric.ServerMetricsMsgCodec;
import edu.snu.cay.services.ps.metric.ServerMetricsMsgSender;
import edu.snu.cay.services.ps.ns.EndpointId;
import edu.snu.cay.services.ps.ns.PSNetworkSetup;
import edu.snu.cay.utils.StateMachine;
import edu.snu.cay.utils.trace.HTrace;
import edu.snu.cay.utils.trace.HTraceParameters;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.ProgressProvider;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.data.loading.api.DataLoadingService;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver code for {@code dolphin-async} applications.
 */
@DriverSide
@Unit
public final class AsyncDolphinDriver {
  private static final Logger LOG = Logger.getLogger(AsyncDolphinDriver.class.getName());
  private static final String WORKER_CONTEXT_ID_PREFIX = "WorkerContext";
  private static final String SERVER_CONTEXT_ID_PREFIX = "ServerContext";
  private static final String WORKER_EM_IDENTIFIER = "WorkerEM";
  private static final String SERVER_EM_IDENTIFIER = "ServerEM";

  /**
   * A state machine representing the state of job.
   * It is initialized by {@link #initStateMachine()}.
   */
  private final StateMachine jobStateMachine;

  /**
   * Evaluator Manager, a unified path for requesting evaluators.
   * Helps managing REEF events related to evaluator and context.
   */
  private final EvaluatorManager evaluatorManager;

  /**
   * Accessor for data loading service.
   * We can check whether a evaluator is configured with the service or not, and
   * query the number of initial evaluators.
   */
  private final DataLoadingService dataLoadingService;

  /**
   * Synchronize workers by exchanging messages.
   */
  private final SynchronizationManager synchronizationManager;

  /**
   * Manage worker clocks.
   */
  private final ClockManager clockManager;

  /**
   * Exchange messages between the driver and evaluators.
   */
  private final AggregationManager aggregationManager;

  /**
   * Manage metrics (drop metrics if inappropriate) from evaluators.
   */
  private final MetricManager metricManager;

  /**
   * Accessor for parameter server service.
   */
  private final PSDriver psDriver;

  /**
   * Class providing a configuration for HTrace.
   */
  private final HTraceParameters traceParameters;

  /**
   * The initial number of worker-side evaluators.
   */
  private final int initWorkerCount;

  /**
   * The initial number of server-side evaluators.
   */
  private final int initServerCount;

  /**
   * Counter for incrementally indexing contexts of workers.
   */
  private final AtomicInteger workerContextIndexCounter = new AtomicInteger(0);

  /**
   * Counter for incrementally indexing contexts of servers.
   */
  private final AtomicInteger serverContextIndexCounter = new AtomicInteger(0);

  /**
   * Bookkeeping the AllocatedEvaluator objects of the evaluators allocated for the job.
   */
  private final Set<AllocatedEvaluator> allocatedEvaluators = Collections.newSetFromMap(new ConcurrentHashMap<>());

  /**
   * Bookkeeping the ActiveContext objects of the root context of both server and worker evaluators.
   */
  private final Map<String, ActiveContext> contextIdToRootContexts = new ConcurrentHashMap<>();

  /**
   * Bookkeeping the ActiveContext objects of the context housing the parameter server.
   */
  private final Map<String, ActiveContext> contextIdToServerContexts = new ConcurrentHashMap<>();

  /**
   * Bookkeeping the ActiveContext objects of the context that the worker task is running on.
   */
  private final Map<String, ActiveContext> contextIdToWorkerContexts = new ConcurrentHashMap<>();

  /**
   * Bookkeeping the RunningTask objects of the running task of workers.
   */
  private final Map<String, RunningTask> contextIdToWorkerTasks = new ConcurrentHashMap<>();

  /**
   * Bookkeeping of context id of workers deleted by EM's Delete.
   */
  private final Set<String> deletedWorkerContextIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

  /**
   * Bookkeeping of context id of servers deleted by EM's Delete.
   */
  private final Set<String> deletedServerContextIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

  /**
   * Configuration that should be passed to each parameter server.
   */
  private final Configuration serverConf;

  /**
   * Configuration that should be passed to each {@link AsyncWorkerTask}.
   */
  private final Configuration workerConf;

  /**
   * EM client configuration, that should be passed to both worker and server EM contexts.
   */
  private final Configuration paramConf;

  /**
   * Worker-side EM client configuration, that should be passed to worker EM contexts.
   */
  private final Configuration emWorkerClientConf;

  /**
   * Server-side EM client configuration, that should be passed to server EM contexts.
   */
  private final Configuration emServerClientConf;

  /**
   * Factory used when establishing network connection.
   */
  private final IdentifierFactory identifierFactory;

  /**
   * The Driver's identifier in String.
   */
  private final String driverIdStr;

  /**
   * A Wrapper object of ElasticMemory for Workers.
   */
  private final EMWrapper workerEMWrapper;

  /**
   * A Wrapper object of ElasticMemory for Servers.
   */
  private final EMWrapper serverEMWrapper;

  /**
   * To establish connections between the Driver and workers/servers.
   */
  private final PSNetworkSetup psNetworkSetup;

  /**
   * Allows PS to get the routing table from EM driver.
   */
  private final EMRoutingTableManager emRoutingTableManager;

  /**
   * Optimization Orchestrator for Dolphin Async.
   */
  private final OptimizationOrchestrator optimizationOrchestrator;

  /**
   * The interval to trigger optimization in millisecond.
   */
  private final long optimizationIntervalMs;

  private final int iterations;
  /**
   * Triggers optimization. Optimization is performed only when workers are running their main iterations.
   * Every optimization is triggered after {@link OptimizationIntervalMs} from the previous optimization.
   * See {@link StartHandler}.
   */
  private final ExecutorService optimizationTriggerExecutor = Executors.newSingleThreadExecutor();

  /**
   * Injectable constructor.
   *
   * The {@code metricManager} parameter is placed here to make sure that {@link OptimizationOrchestratorImpl},
   * {@link edu.snu.cay.dolphin.async.metric.DriverSideMetricsMsgHandlerForWorker}, and
   * {@link edu.snu.cay.dolphin.async.metric.DriverSideMetricsMsgHandlerForServer} hold references to the same
   * {@link MetricManager} instance.
   */
  @Inject
  private AsyncDolphinDriver(final EvaluatorManager evaluatorManager,
                             final DataLoadingService dataLoadingService,
                             final SynchronizationManager synchronizationManager,
                             final ClockManager clockManager,
                             final Injector injector,
                             final IdentifierFactory identifierFactory,
                             @Parameter(DriverIdentifier.class) final String driverIdStr,
                             final AggregationManager aggregationManager,
                             @Parameter(SerializedWorkerConfiguration.class) final String serializedWorkerConf,
                             @Parameter(SerializedParameterConfiguration.class) final String serializedParamConf,
                             @Parameter(SerializedServerConfiguration.class) final String serializedServerConf,
                             @Parameter(SerializedEMWorkerClientConfiguration.class)
                                 final String serializedEMWorkerClientConf,
                             @Parameter(SerializedEMServerClientConfiguration.class)
                                 final String serializedEMServerClientConf,
                             @Parameter(NumServers.class) final int numServers,
                             final ConfigurationSerializer configurationSerializer,
                             @Parameter(Parameters.Iterations.class) final int iterations,
                             @Parameter(OptimizationIntervalMs.class) final long optimizationIntervalMs,
                             final MetricManager metricManager,
                             final HTraceParameters traceParameters,
                             final HTrace hTrace) throws IOException {
    hTrace.initialize();
    this.evaluatorManager = evaluatorManager;
    this.dataLoadingService = dataLoadingService;
    this.synchronizationManager = synchronizationManager;
    this.clockManager = clockManager;
    this.initWorkerCount = dataLoadingService.getNumberOfPartitions();
    this.initServerCount = numServers;
    this.identifierFactory = identifierFactory;
    this.driverIdStr = driverIdStr;
    this.aggregationManager = aggregationManager;
    this.metricManager = metricManager;

    this.workerConf = configurationSerializer.fromString(serializedWorkerConf);
    this.paramConf = configurationSerializer.fromString(serializedParamConf);
    this.serverConf = configurationSerializer.fromString(serializedServerConf);
    this.emWorkerClientConf = configurationSerializer.fromString(serializedEMWorkerClientConf);
    this.emServerClientConf = configurationSerializer.fromString(serializedEMServerClientConf);

    this.traceParameters = traceParameters;
    this.optimizationIntervalMs = optimizationIntervalMs;
    this.iterations = iterations;

    try {
      final Injector workerInjector = injector.forkInjector();
      workerInjector.bindVolatileInstance(EMDeleteExecutor.class, new WorkerRemover());
      workerInjector.bindVolatileParameter(EMIdentifier.class, WORKER_EM_IDENTIFIER);
      workerInjector.bindVolatileParameter(RangeSupport.class, Boolean.TRUE);
      workerInjector.bindVolatileParameter(ConsistencyPreserved.class, Boolean.FALSE);
      this.workerEMWrapper = workerInjector.getInstance(EMWrapper.class);

      final Injector serverInjector = injector.forkInjector();
      serverInjector.bindVolatileInstance(EMDeleteExecutor.class, new ServerRemover());
      serverInjector.bindVolatileParameter(EMIdentifier.class, SERVER_EM_IDENTIFIER);
      serverInjector.bindVolatileParameter(RangeSupport.class, Boolean.FALSE);
      serverInjector.bindVolatileParameter(ConsistencyPreserved.class, Boolean.TRUE);
      this.serverEMWrapper = serverInjector.getInstance(EMWrapper.class);
      this.emRoutingTableManager = serverInjector.getInstance(EMRoutingTableManager.class);
      this.psDriver = serverInjector.getInstance(PSDriver.class);
      this.psNetworkSetup = serverInjector.getInstance(PSNetworkSetup.class);

      this.jobStateMachine = initStateMachine();

      final Injector optimizerInjector = injector.forkInjector();
      optimizerInjector.bindVolatileParameter(ServerEM.class, serverEMWrapper.getInstance());
      optimizerInjector.bindVolatileParameter(WorkerEM.class, workerEMWrapper.getInstance());
      optimizerInjector.bindVolatileInstance(EMRoutingTableManager.class, emRoutingTableManager);
      optimizerInjector.bindVolatileInstance(AsyncDolphinDriver.class, this);
      this.optimizationOrchestrator = optimizerInjector.getInstance(OptimizationOrchestrator.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  private StateMachine initStateMachine() {
    return StateMachine.newBuilder()
        .addState(State.RUNNING, "The job is running")
        .addState(State.CLOSING_WORKERS, "The job is started being closed, first closing the worker contexts")
        .addState(State.CLOSING_SERVERS, "Closing the server contexts")
        .addState(State.CLOSING_ROOT_CONTEXTS, "Closing root contexts of both server and worker")
        .setInitialState(State.RUNNING)
        .addTransition(State.RUNNING, State.CLOSING_WORKERS,
            "The job is finished, time to close the worker contexts")
        .addTransition(State.CLOSING_WORKERS, State.CLOSING_SERVERS,
            "Worker contexts are finished, time to close the the server contexts")
        .addTransition(State.CLOSING_SERVERS, State.CLOSING_ROOT_CONTEXTS,
            "Both worker and server contexts are finished, time to close their root contexts")
        .build();
  }

  private enum State {
    RUNNING,
    CLOSING_WORKERS,
    CLOSING_SERVERS,
    CLOSING_ROOT_CONTEXTS,
  }

  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "StartTime: {0}", startTime);

      final EventHandler<AllocatedEvaluator> evalAllocHandlerForServer = getEvalAllocHandlerForServer();
      final List<EventHandler<ActiveContext>> contextActiveHandlersForServer = new ArrayList<>(2);
      contextActiveHandlersForServer.add(getFirstContextActiveHandlerForServer(false));
      contextActiveHandlersForServer.add(getSecondContextActiveHandlerForServer());
      evaluatorManager.allocateEvaluators(initServerCount, evalAllocHandlerForServer, contextActiveHandlersForServer);

      final EventHandler<AllocatedEvaluator> evalAllocHandlerForWorker = getEvalAllocHandlerForWorker();
      final List<EventHandler<ActiveContext>> contextActiveHandlersForWorker = new ArrayList<>(2);
      contextActiveHandlersForWorker.add(getFirstContextActiveHandlerForWorker(false));
      contextActiveHandlersForWorker.add(getSecondContextActiveHandlerForWorker(false));
      evaluatorManager.allocateEvaluators(dataLoadingService.getNumberOfPartitions(),
          evalAllocHandlerForWorker, contextActiveHandlersForWorker);

      // Register the driver to the Network.
      final Identifier driverId = identifierFactory.getNewInstance(driverIdStr);
      workerEMWrapper.getNetworkSetup().registerConnectionFactory(driverId);
      serverEMWrapper.getNetworkSetup().registerConnectionFactory(driverId);
      psNetworkSetup.registerConnectionFactory(driverId);

      optimizationTriggerExecutor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            // 1. wait until all workers finish initialization
            while (synchronizationManager.workersInitializing()) {
              try {
                synchronizationManager.waitInitialization();
              } catch (final InterruptedException e) {
                LOG.log(Level.WARNING, "Interrupted while waiting for the worker initialization", e);
              }
            }

            // 2. Start collecting metrics from evaluators.
            //    Load metric manager's data validation map using ServerEM and WorkerEM, which are initialized by now.
            metricManager.loadMetricValidationInfo(workerEMWrapper.getInstance().getEvalIdToNumBlocks(),
                serverEMWrapper.getInstance().getEvalIdToNumBlocks());
            metricManager.startMetricCollection();

            LOG.log(Level.INFO, "Worker tasks are initialized. Start triggering optimization.");

            // 3. trigger optimization during all workers are running their main iterations
            // synchronizationManager.waitingCleanup() becomes true when any workers have finished their main iterations
            LOG.log(Level.FINE, "Trigger optimization with interval {0} ms", optimizationIntervalMs);
            while (!synchronizationManager.waitingCleanup()) {
              optimizationOrchestrator.run();
              try {
                Thread.sleep(optimizationIntervalMs);
              } catch (final InterruptedException e) {
                LOG.log(Level.WARNING, "Interrupted while sleeping between optimizations", e);
              }
            }
          } catch (final RuntimeException e) {
            LOG.log(Level.SEVERE, "RuntimeException from optimization triggering thread", e);
            throw e;

          } finally {
            // 4. allow workers to do cleanup, after finishing optimization entirely
            LOG.log(Level.INFO, "Stop triggering optimization. Allow workers do cleanup");
            synchronizationManager.allowWorkersCleanup();

            // 5. Stop metric collection
            metricManager.stopMetricCollection();
          }
        }
      });
    }
  }

  private String getWorkerContextId(final int workerIndex) {
    return WORKER_CONTEXT_ID_PREFIX + "-" + workerIndex;
  }

  private String getServerContextId(final int serverIndex) {
    return SERVER_CONTEXT_ID_PREFIX + "-" + serverIndex;
  }

  private String getWorkerTaskId(final int workerIndex) {
    return AsyncWorkerTask.TASK_ID_PREFIX + "-" + workerIndex;
  }

  private int getWorkerIndex(final String contextId) {
    return Integer.parseInt(contextId.substring(WORKER_CONTEXT_ID_PREFIX.length() + 1));
  }

  /**
   * Returns an EventHandler which submits the first context(DataLoading compute context) to server-side evaluator.
   */
  public EventHandler<AllocatedEvaluator> getEvalAllocHandlerForServer() {
    return new EventHandler<AllocatedEvaluator>() {
      @Override
      public void onNext(final AllocatedEvaluator allocatedEvaluator) {
        LOG.log(Level.FINE, "Submitting Compute Context to {0}", allocatedEvaluator.getId());
        allocatedEvaluators.add(allocatedEvaluator);

        final int serverIndex = serverContextIndexCounter.getAndIncrement();
        final Configuration idConfiguration = ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, dataLoadingService.getComputeContextIdPrefix() + serverIndex)
            .build();
        allocatedEvaluator.submitContext(idConfiguration);
      }
    };
  }

  /**
   * Returns an EventHandler which submits the first context(DataLoading context) to worker-side evaluator.
   */
  public EventHandler<AllocatedEvaluator> getEvalAllocHandlerForWorker() {
    return new EventHandler<AllocatedEvaluator>() {
      @Override
      public void onNext(final AllocatedEvaluator allocatedEvaluator) {
        allocatedEvaluators.add(allocatedEvaluator);

        LOG.log(Level.FINE, "Submitting data loading context to {0}", allocatedEvaluator.getId());
        allocatedEvaluator.submitContextAndService(dataLoadingService.getContextConfiguration(allocatedEvaluator),
            dataLoadingService.getServiceConfiguration(allocatedEvaluator));
      }
    };
  }

  /**
   * Returns an EventHandler which submits the second context(parameter server context) to server-side evaluator.
   */
  public EventHandler<ActiveContext> getFirstContextActiveHandlerForServer(final boolean addedEval) {
    return new EventHandler<ActiveContext>() {
      @Override
      public void onNext(final ActiveContext activeContext) {
        LOG.log(Level.INFO, "Server-side Compute context - {0}", activeContext);
        contextIdToRootContexts.put(activeContext.getId(), activeContext);

        final int serverIndex = Integer.parseInt(
            activeContext.getId().substring(dataLoadingService.getComputeContextIdPrefix().length()));
        final String contextId = getServerContextId(serverIndex);
        final Configuration contextConf = Configurations.merge(
            ContextConfiguration.CONF
                .set(ContextConfiguration.IDENTIFIER, contextId)
                .build(),
            psDriver.getServerContextConfiguration(),
            serverEMWrapper.getConf().getContextConfiguration(),
            aggregationManager.getContextConfiguration());
        final Configuration serviceConf = Configurations.merge(
            psDriver.getServerServiceConfiguration(contextId),
            Tang.Factory.getTang().newConfigurationBuilder(
                serverEMWrapper.getConf().getServiceConfigurationWithoutNameResolver(contextId, initServerCount))
                .bindNamedParameter(AddedEval.class, String.valueOf(addedEval))
                .build(),
            aggregationManager.getServiceConfigurationWithoutNameResolver(),
            getMetricsCollectionServiceConfForServer());

        final Injector serviceInjector = Tang.Factory.getTang().newInjector(serviceConf);
        try {
          // to synchronize EM's MemoryStore id and PS's Network endpoint.
          final int memoryStoreId = serviceInjector.getNamedInstance(MemoryStoreId.class);
          final String endpointId = serviceInjector.getNamedInstance(EndpointId.class);
          emRoutingTableManager.registerServer(memoryStoreId, endpointId);
        } catch (final InjectionException e) {
          throw new RuntimeException(e);
        }

        final Configuration traceConf = traceParameters.getConfiguration();

        activeContext.submitContextAndService(contextConf,
            Configurations.merge(serviceConf, traceConf, paramConf, serverConf, emServerClientConf));
      }
    };
  }

  /**
   * Returns an EventHandler which finishes server-side evaluator setup.
   */
  public EventHandler<ActiveContext> getSecondContextActiveHandlerForServer() {
    return new EventHandler<ActiveContext>() {
      @Override
      public void onNext(final ActiveContext activeContext) {
        LOG.log(Level.INFO, "Server-side ParameterServer context - {0}", activeContext);
        contextIdToServerContexts.put(activeContext.getId(), activeContext);
      }
    };
  }

  /**
   * Returns an EventHandler which submits the second context(worker context) to worker-side evaluator.
   */
  public EventHandler<ActiveContext> getFirstContextActiveHandlerForWorker(final boolean addedEval) {
    return new EventHandler<ActiveContext>() {
      @Override
      public void onNext(final ActiveContext activeContext) {
        LOG.log(Level.INFO, "Worker-side DataLoad context - {0}", activeContext);
        contextIdToRootContexts.put(activeContext.getId(), activeContext);

        final int workerIndex = workerContextIndexCounter.getAndIncrement();
        final String contextId = getWorkerContextId(workerIndex);
        final Configuration contextConf = Configurations.merge(
            ContextConfiguration.CONF
                .set(ContextConfiguration.IDENTIFIER, contextId)
                .build(),
            psDriver.getWorkerContextConfiguration(),
            workerEMWrapper.getConf().getContextConfiguration(),
            aggregationManager.getContextConfiguration());

        final Configuration serviceConf = Configurations.merge(
            psDriver.getWorkerServiceConfiguration(contextId),
            getEMServiceConfForWorker(contextId, addedEval),
            aggregationManager.getServiceConfigurationWithoutNameResolver(),
            getMetricsCollectionServiceConfForWorker());
        final Configuration traceConf = traceParameters.getConfiguration();

        // TODO #681: Need to add configuration for numWorkerThreads after multi-thread worker is enabled
        activeContext.submitContextAndService(contextConf,
            Configurations.merge(serviceConf, traceConf, paramConf, workerConf, emWorkerClientConf));
      }
    };
  }

  /**
   * Returns server-side Configuration for MetricsCollectionService by binding required parameters.
   */
  private Configuration getMetricsCollectionServiceConfForServer() {
    final MetricsCollectionServiceConf conf = MetricsCollectionServiceConf.newBuilder()
        .setMetricsHandlerClass(ServerMetricsMsgSender.class)
        .setMetricsMsgSenderClass(ServerMetricsMsgSender.class)
        .setMetricsMsgCodecClass(ServerMetricsMsgCodec.class)
        .build();
    return conf.getConfiguration();
  }

  /**
   * Returns worker-side Configuration for MetricsCollectionService by binding required parameters.
   */
  private Configuration getMetricsCollectionServiceConfForWorker() {
    final MetricsCollectionServiceConf conf = MetricsCollectionServiceConf.newBuilder()
        .setMetricsHandlerClass(WorkerMetricsMsgSender.class)
        .setMetricsMsgSenderClass(WorkerMetricsMsgSender.class)
        .setMetricsMsgCodecClass(WorkerMetricsMsgCodec.class)
        .build();
    return conf.getConfiguration();
  }

  /**
   * Returns Worker-side configuration for EM Service.
   */
  private Configuration getEMServiceConfForWorker(final String contextId, final boolean addedEval) {
    if (addedEval) {
      return Tang.Factory.getTang().newConfigurationBuilder(
          workerEMWrapper.getConf().getServiceConfigurationWithoutNameResolver(contextId, initWorkerCount))
          .bindNamedParameter(AddedEval.class, Boolean.toString(true))
          .bindImplementation(DataSet.class, EmptyDataSet.class) // If not set, Tang fails while injecting Task.
          .build();
    } else {
      return Tang.Factory.getTang().newConfigurationBuilder(
          workerEMWrapper.getConf().getServiceConfigurationWithoutNameResolver(contextId, initWorkerCount))
          .bindNamedParameter(AddedEval.class, Boolean.toString(false))
          .build();
    }
  }

  /**
   * Returns an EventHandler which submits worker task to worker-side evaluator.
   */
  public EventHandler<ActiveContext> getSecondContextActiveHandlerForWorker(final boolean addedEval) {
    return new EventHandler<ActiveContext>() {
      @Override
      public void onNext(final ActiveContext activeContext) {
        LOG.log(Level.INFO, "Worker-side ParameterWorker context - {0}", activeContext);
        contextIdToWorkerContexts.put(activeContext.getId(), activeContext);

        // notify SyncManager about the addition of worker
        synchronizationManager.onWorkerAdded();

        clockManager.onWorkerAdded(addedEval, activeContext.getId());
        final int workerIndex = getWorkerIndex(activeContext.getId());
        final Configuration taskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, getWorkerTaskId(workerIndex))
            .set(TaskConfiguration.TASK, AsyncWorkerTask.class)
            .set(TaskConfiguration.ON_CLOSE, AsyncWorkerTask.CloseEventHandler.class)
            .build();
        final Configuration emDataIdConf = Tang.Factory.getTang().newConfigurationBuilder()
            .bindImplementation(DataIdFactory.class, RoundRobinDataIdFactory.class).build();

        activeContext.submitTask(Configurations.merge(taskConf, emDataIdConf));
      }
    };
  }

  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "AllocatedEvaluator: {0}", allocatedEvaluator);
      evaluatorManager.onEvaluatorAllocated(allocatedEvaluator);
    }
  }

  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext activeContext) {
      LOG.log(Level.INFO, "ActiveContext: {0}", activeContext);
      evaluatorManager.onContextActive(activeContext);
    }
  }

  final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask runningTask) {
      LOG.log(Level.INFO, "RunningTask: {0}", runningTask);
      contextIdToWorkerTasks.put(runningTask.getActiveContext().getId(), runningTask);
      optimizationOrchestrator.onRunningTask(runningTask);
    }
  }

  /**
   * Handler for FailedEvaluator, which throws RuntimeException to shutdown the entire job.
   */
  final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      // TODO #677: Handle failure from Evaluators properly
      throw new RuntimeException(failedEvaluator.getEvaluatorException());
    }
  }

  final class CompletedEvaluatorHandler implements EventHandler<CompletedEvaluator> {
    @Override
    public void onNext(final CompletedEvaluator completedEvaluator) {
      LOG.log(Level.FINE, "CompletedEvaluator: {0}", completedEvaluator); // just for logging
    }
  }

  /**
   * Handler for FailedContext, which throws RuntimeException to shutdown the entire job.
   */
  final class FailedContextHandler implements EventHandler<FailedContext> {
    @Override
    public void onNext(final FailedContext failedContext) {
      // TODO #677: Handle failure from Evaluators properly
      throw new RuntimeException(failedContext.asError());
    }
  }

  /**
   * Handler for ClosedContext.
   */
  final class ClosedContextHandler implements EventHandler<ClosedContext> {
    @Override
    public void onNext(final ClosedContext closedContext) {
      LOG.log(Level.INFO, "ClosedContext: {0}", closedContext);
      final String contextId = closedContext.getId();

      handleFinishedContext(contextId, Optional.of(closedContext.getParentContext()));
    }
  }

  private synchronized void handleFinishedWorkerContext(final String contextId) {
    contextIdToWorkerContexts.remove(contextId);

    final State jobState = (State) jobStateMachine.getCurrentState();
    switch (jobState) {
    case RUNNING:
      LOG.log(Level.WARNING, "Worker context {0} is closed when the job is not in shutdown phase", contextId);
      break;
    case CLOSING_WORKERS:
      if (contextIdToWorkerContexts.isEmpty()) { // all worker contexts are closed
        jobStateMachine.setState(State.CLOSING_SERVERS);

        LOG.info("Start closing server contexts");
        for (final ActiveContext serverContext : contextIdToServerContexts.values()) {
          serverContext.close();
        }
      }
      break;
    case CLOSING_SERVERS:
      LOG.log(Level.WARNING, "Worker context {0} is closed after starting close of servers", contextId);
      break;
    case CLOSING_ROOT_CONTEXTS:
      LOG.log(Level.WARNING, "Worker context {0} is closed after starting close of root contexts", contextId);
      break;
    default:
      throw new RuntimeException("Unexpected state");
    }
  }

  private synchronized void handleFinishedServerContext(final String contextId) {
    contextIdToServerContexts.remove(contextId);

    final State jobState = (State) jobStateMachine.getCurrentState();
    switch (jobState) {
    case RUNNING:
      LOG.log(Level.WARNING, "Server context {0} is closed when the job is not in shutdown phase", contextId);
      break;
    case CLOSING_WORKERS:
      LOG.log(Level.WARNING, "Server context {0} is closed before starting close of servers", contextId);
      break;
    case CLOSING_SERVERS:
      if (contextIdToServerContexts.isEmpty()) { // all server contexts are closed
        jobStateMachine.setState(State.CLOSING_ROOT_CONTEXTS);

        LOG.info("Start closing root contexts");
        for (final ActiveContext rootContext : contextIdToRootContexts.values()) {
          rootContext.close();
        }
      }
      break;
    case CLOSING_ROOT_CONTEXTS:
      LOG.log(Level.WARNING, "Server context {0} is closed after starting close of root contexts", contextId);
      break;
    default:
      throw new RuntimeException("Unexpected state");
    }
  }

  /**
   * Handler for finished contexts, including ClosedContext and FailedContext.
   * The contexts should be one of following:
   *  1) contexts closed in job shutdown phase started by {@link #checkShutdown()}
   *  2) contexts closed by EM's Delete procedure
   *
   * For an exceptional context out of upper cases, it closes the parent of the context, if it exist,
   * to prevent the job from holding unnecessary resources.
   * It is also for the job to be shut down in unexpected cases.
   * @param contextId an identifier of the context
   */
  private void handleFinishedContext(final String contextId, final Optional<ActiveContext> parentContext) {
    // handle contexts corresponding to their type
    // case1. Worker context is finished
    if (contextIdToWorkerContexts.containsKey(contextId)) {
      handleFinishedWorkerContext(contextId);

    // case2. Server context is finished
    } else if (contextIdToServerContexts.containsKey(contextId)) {
      handleFinishedServerContext(contextId);

    // case3. root context is finished
    } else if (contextIdToRootContexts.remove(contextId) != null) {
      // for tracking contexts
      LOG.log(Level.INFO, "Root context {0} is closed. Its evaluator will be released soon.", contextId);

    // case4-1. worker context is finished by EM's delete
    } else if (deletedWorkerContextIds.remove(contextId)) {
      LOG.log(Level.INFO, "The worker context {0} is closed by EM's Delete.", contextId);

      // notify SyncManager about the deletion of worker
      synchronizationManager.onWorkerDeleted(contextId);

      // notify ClockManager about the deletion of worker
      clockManager.onWorkerDeleted(contextId);

      if (!parentContext.isPresent()) {
        throw new RuntimeException("Root context of the deleted worker context does not exist");
      }
      closeParentRootContext(parentContext.get());

    // case4-2. server context is finished by EM's delete
    } else if (deletedServerContextIds.remove(contextId)) {
      LOG.log(Level.INFO, "The server context {0} is closed by EM's Delete.", contextId);

      // notify RoutingTableManager about the deletion of server
      emRoutingTableManager.deregisterServer(contextId);

      if (!parentContext.isPresent()) {
        throw new RuntimeException("Root context of the deleted server context does not exist");
      }
      closeParentRootContext(parentContext.get());

    // case5. untracked context is finished
    } else {
      LOG.log(Level.WARNING, "Untracked context: {0}", contextId);
      if (parentContext.isPresent()) {
        LOG.log(Level.INFO, "Close a context {0}, which is a parent of the finished context whose id is {1}",
            new Object[]{parentContext, contextId});
        parentContext.get().close();
      }
    }
  }

  private void closeParentRootContext(final ActiveContext parentContext) {
    // the deleted worker/server context was running on the root context, a data loading context.
    // we should close the root context to completely release the evaluator on which the deleted worker/server has run
    final String rootContextId = parentContext.getId();
    final ActiveContext rootContext = contextIdToRootContexts.remove(rootContextId);
    rootContext.close();
  }

  /**
   * Handler for FailedTask, which throws RuntimeException to shutdown the entire job.
   */
  final class FailedTaskHandler implements EventHandler<FailedTask> {
    @Override
    public void onNext(final FailedTask failedTask) {
      // TODO #677: Handle failure from Evaluators properly
      throw new RuntimeException(failedTask.asError());
    }
  }

  /**
   * Handler for CompletedTask.
   * It does not close the context right away, because these evaluators still have valid data,
   * which can be accessed by other running evaluators.
   * It starts shutting down whole existing contexts and evaluators when there are no running tasks.
   * See {@link #checkShutdown()}.
   */
  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask completedTask) {
      LOG.log(Level.INFO, "CompletedTask: {0}", completedTask);

      contextIdToWorkerTasks.remove(completedTask.getActiveContext().getId());
      optimizationOrchestrator.onCompletedTask(completedTask);

      checkShutdown();
    }
  }

  /**
   * Start job shutdown, if all worker tasks are finished.
   * To shutdown job completely, following steps have to be done in sequence after completing previous step.
   *  1) Close the worker contexts first, since worker-server communication is driven by worker
   *  2) Close the server contexts
   *  3) Close the root contexts of both server and worker
   *
   * First step is done by this method.
   * Second and third steps are done in ClosedContextHandler, triggered by resulting events of the first step.
   * If a previous step is not completed within time bound, it progresses to the next step.
   */
  private synchronized void checkShutdown() {
    if (!contextIdToWorkerTasks.isEmpty()) {
      return;
    }

    if (jobStateMachine.compareAndSetState(State.RUNNING, State.CLOSING_WORKERS)) {
      LOG.log(Level.INFO, "Time to shut down the job");

      // start shutdown thread
      Executors.newSingleThreadExecutor().submit(new Runnable() {

        /**
         * Interval between trials to shutdown contexts to make sure
         * no optimization plan is executing.
         */
        private static final long SHUTDOWN_TRIAL_INTERVAL_MS = 3000;
        private static final long CONTEXT_CLOSE_GRACE_PERIOD_MS = 10000;

        @Override
        public void run() {
          while (optimizationOrchestrator.isPlanExecuting()) {
            try {
              LOG.log(Level.INFO, "It's time to shutdown active contexts, but wait for completion of plan execution");
              Thread.sleep(SHUTDOWN_TRIAL_INTERVAL_MS);
            } catch (final InterruptedException e) {
              LOG.log(Level.WARNING, "Interrupted while waiting for plan finish", e);
            }
          }

          LOG.log(Level.INFO, "Shutdown worker contexts");
          // Since it is not guaranteed that the messages from workers are completely received and processed,
          // the servers may lose some updates.
          for (final ActiveContext workerContext : contextIdToWorkerContexts.values()) {
            workerContext.close();
          }

          // wait for worker contexts to be closed
          try {
            Thread.sleep(CONTEXT_CLOSE_GRACE_PERIOD_MS);
          } catch (InterruptedException e) {
            LOG.log(Level.WARNING, "Interrupted while waiting for close of worker contexts", e);
          }

          if (jobStateMachine.compareAndSetState(State.CLOSING_WORKERS, State.CLOSING_SERVERS)) {
            LOG.log(Level.WARNING, "Some workers do not respond to close messages within time." +
                " Start closing server contexts.");

            // ignore remaining worker contexts and close server contexts
            for (final ActiveContext serverContext : contextIdToServerContexts.values()) {
              serverContext.close();
            }
          }

          try {
            Thread.sleep(CONTEXT_CLOSE_GRACE_PERIOD_MS);
          } catch (InterruptedException e) {
            LOG.log(Level.WARNING, "Interrupted while waiting for close of server contexts", e);
          }

          if (jobStateMachine.compareAndSetState(State.CLOSING_SERVERS, State.CLOSING_ROOT_CONTEXTS)) {
            LOG.log(Level.WARNING, "Some servers do not respond to close messages within time." +
                " Start closing root contexts.");

            // ignore remaining server contexts and close root contexts
            for (final ActiveContext rootContext : contextIdToRootContexts.values()) {
              rootContext.close();
            }
          }

          try {
            Thread.sleep(CONTEXT_CLOSE_GRACE_PERIOD_MS);
          } catch (InterruptedException e) {
            LOG.log(Level.WARNING, "Interrupted while waiting for close of root contexts", e);
          }

          LOG.log(Level.INFO, "Some root contexts do not respond to close message. Close evaluators directly.");
          // If I am still alive, then kill evaluators directly via AllocatedEvaluator.close(), which uses RM
          for (final AllocatedEvaluator allocatedEvaluator : allocatedEvaluators) {
            allocatedEvaluator.close();
          }
        }
      });
    } else {
      LOG.log(Level.WARNING, "The Job is already being shutdown. Driver fails to correctly track worker tasks.");
    }
  }

  /**
   * Deletes Servers by closing active contexts that the Servers are run on.
   * Should be package private (not private), since Tang complains Explicit constructor in @Unit class.
   */
  final class ServerRemover implements EMDeleteExecutor {
    @Override
    public boolean execute(final String activeContextId, final EventHandler<MigrationMsg> callback) {
      final ActiveContext activeContext = contextIdToServerContexts.remove(activeContextId);
      final boolean isSuccess;
      if (activeContext == null) {
        LOG.log(Level.WARNING,
            "Trying to remove active context {0}, which is not found", activeContextId);
        isSuccess = false;
      } else {
        deletedServerContextIds.add(activeContextId);

        activeContext.close();
        LOG.log(Level.FINE, "Server has been deleted successfully. Remaining Servers: {0}",
            contextIdToServerContexts.size());
        isSuccess = true;
      }
      sendCallback(activeContextId, callback, isSuccess);
      return isSuccess;
    }
  }

  /**
   * Closes the running task on the context whose id is {@code workerContextId}.
   * PlanExecutor invokes this method to stop task before migrating out existing data.
   * @param workerContextId an id of worker context, where the task is running on
   * @return true if succeeds to close
   */
  public boolean closeWorkerTask(final String workerContextId) {
    final RunningTask runningTask = contextIdToWorkerTasks.remove(workerContextId);
    final boolean isSuccess;
    if (runningTask == null) {
      LOG.log(Level.WARNING,
          "Trying to close running task on {0}, which is not found", workerContextId);
      isSuccess = false;
    } else {
      // context will be closed in WorkerRemover
      runningTask.close();
      isSuccess = true;
    }
    return isSuccess;
  }

  /**
   * Deletes Workers by closing active contexts that the Workers are run on.
   * Should be package private (not private), since Tang complains Explicit constructor in @Unit class.
   */
  final class WorkerRemover implements EMDeleteExecutor {
    @Override
    public boolean execute(final String activeContextId, final EventHandler<MigrationMsg> callback) {
      final ActiveContext activeContext = contextIdToWorkerContexts.remove(activeContextId);
      final boolean isSuccess;
      if (activeContext == null) {
        LOG.log(Level.WARNING,
            "Trying to remove active context {0}, which is not found", activeContextId);
        isSuccess = false;
      } else {
        deletedWorkerContextIds.add(activeContextId);

        activeContext.close();
        LOG.log(Level.FINE, "Worker has been deleted successfully. Remaining workers: {0}",
            contextIdToWorkerContexts.size());
        isSuccess = true;
      }
      sendCallback(activeContextId, callback, isSuccess);
      return isSuccess;
    }
  }

  /**
   * Invokes callback message to notify the result of EM operations.
   */
  // TODO #205: Reconsider using of Avro message in EM's callback
  private void sendCallback(final String contextId,
                            final EventHandler<MigrationMsg> callback,
                            final boolean isSuccess) {
    final ResultMsg resultMsg = ResultMsg.newBuilder()
        .setResult(isSuccess ? Result.SUCCESS : Result.FAILURE)
        .setSrcId(contextId)
        .build();

    final MigrationMsg migrationMsg = MigrationMsg.newBuilder()
        .setType(MigrationMsgType.ResultMsg)
        .setResultMsg(resultMsg)
        .build();

    callback.onNext(migrationMsg);
  }

  /**
   * Tracks the job's progress, which is reported to the REEF Driver.
   */
  final class DolphinProgressProvider implements ProgressProvider {

    @Override
    public float getProgress() {
      // TODO #830: Once we change clock to tick every mini-batch instead of epoch, we should change below accordingly.
      final int minClock = clockManager.getGlobalMinimumClock();
      return (float) minClock / iterations;
    }
  }
}
