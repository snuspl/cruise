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

import edu.snu.cay.dolphin.async.AsyncDolphinLauncher.*;
import edu.snu.cay.dolphin.async.optimizer.EmptyDataSet;
import edu.snu.cay.dolphin.async.optimizer.ServerEM;
import edu.snu.cay.dolphin.async.optimizer.parameters.OptimizationIntervalMs;
import edu.snu.cay.dolphin.async.metric.MetricsCollectionService;
import edu.snu.cay.dolphin.async.optimizer.OptimizationOrchestrator;
import edu.snu.cay.dolphin.async.optimizer.WorkerEM;
import edu.snu.cay.common.aggregation.driver.AggregationManager;
import edu.snu.cay.common.param.Parameters.NumWorkerThreads;
import edu.snu.cay.services.em.avro.AvroElasticMemoryMessage;
import edu.snu.cay.services.em.avro.Result;
import edu.snu.cay.services.em.avro.ResultMsg;
import edu.snu.cay.services.em.avro.Type;
import edu.snu.cay.services.em.common.parameters.AddedEval;
import edu.snu.cay.services.em.common.parameters.MemoryStoreId;
import edu.snu.cay.services.em.common.parameters.RangeSupport;
import edu.snu.cay.services.em.driver.EMWrapper;
import edu.snu.cay.services.em.driver.api.EMDeleteExecutor;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.impl.RoundRobinDataIdFactory;
import edu.snu.cay.services.em.ns.parameters.EMIdentifier;
import edu.snu.cay.services.evalmanager.api.EvaluatorManager;
import edu.snu.cay.services.ps.common.parameters.NumServers;
import edu.snu.cay.services.ps.driver.impl.PSDriver;
import edu.snu.cay.services.ps.driver.impl.EMRoutingTableManager;
import edu.snu.cay.services.ps.ns.EndpointId;
import edu.snu.cay.services.ps.ns.PSNetworkSetup;
import edu.snu.cay.utils.StateMachine;
import edu.snu.cay.utils.trace.HTrace;
import edu.snu.cay.utils.trace.HTraceParameters;
import org.apache.reef.annotations.audience.DriverSide;
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
  private static final String WORKER_CONTEXT_ID = "WorkerContext";
  private static final String SERVER_CONTEXT_ID = "ServerContext";
  private static final String WORKER_EM_IDENTIFIER = "WorkerEM";
  private static final String SERVER_EM_IDENTIFIER = "ServerEM";

  // states of jobStateMachine
  private static final String RUNNING = "RUNNING";
  private static final String CLOSING_WORKERS = "CLOSING_WORKERS";
  private static final String CLOSING_SERVERS = "CLOSING_SERVERS";
  private static final String CLOSING_ROOTS = "CLOSING_ROOTS";

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
   * Exchange messages between the driver and evaluators.
   */
  private final AggregationManager aggregationManager;

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
  private final Map<String, AllocatedEvaluator> allocatedEvaluators = new ConcurrentHashMap<>();

  /**
   * Bookkeeping the ActiveContext objects of the root context of both server and worker evaluators.
   */
  private final Map<String, ActiveContext> rootContexts = new ConcurrentHashMap<>();

  /**
   * Bookkeeping the ActiveContext objects of the context housing the parameter server.
   */
  private final Map<String, ActiveContext> serverContexts = new ConcurrentHashMap<>();

  /**
   * Bookkeeping the ActiveContext objects of the context that the worker task is running on.
   */
  private final Map<String, ActiveContext> workerContexts = new ConcurrentHashMap<>();

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
   * Number of computation threads for each evaluator.
   */
  private final int numWorkerThreads;

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

  /**
   * Triggers optimization. After waiting an initial delay,
   * optimization is performed periodically for now (See {@link StartHandler}).
   */
  private final ExecutorService optimizerExecutor = Executors.newSingleThreadExecutor();

  @Inject
  private AsyncDolphinDriver(final EvaluatorManager evaluatorManager,
                             final DataLoadingService dataLoadingService,
                             final Injector injector,
                             final IdentifierFactory identifierFactory,
                             @Parameter(DriverIdentifier.class) final String driverIdStr,
                             final AggregationManager aggregationManager,
                             @Parameter(SerializedWorkerConfiguration.class) final String serializedWorkerConf,
                             @Parameter(SerializedParameterConfiguration.class) final String serializedParamConf,
                             @Parameter(SerializedEMWorkerClientConfiguration.class)
                                 final String serializedEMWorkerClientConf,
                             @Parameter(SerializedEMServerClientConfiguration.class)
                                 final String serializedEMServerClientConf,
                             @Parameter(NumServers.class) final int numServers,
                             final ConfigurationSerializer configurationSerializer,
                             @Parameter(NumWorkerThreads.class) final int numWorkerThreads,
                             @Parameter(OptimizationIntervalMs.class) final long optimizationIntervalMs,
                             final HTraceParameters traceParameters,
                             final HTrace hTrace) throws IOException {
    hTrace.initialize();
    this.evaluatorManager = evaluatorManager;
    this.dataLoadingService = dataLoadingService;
    this.initWorkerCount = dataLoadingService.getNumberOfPartitions();
    this.initServerCount = numServers;
    this.identifierFactory = identifierFactory;
    this.driverIdStr = driverIdStr;
    this.aggregationManager = aggregationManager;
    this.workerConf = configurationSerializer.fromString(serializedWorkerConf);
    this.paramConf = configurationSerializer.fromString(serializedParamConf);
    this.emWorkerClientConf = configurationSerializer.fromString(serializedEMWorkerClientConf);
    this.emServerClientConf = configurationSerializer.fromString(serializedEMServerClientConf);

    this.numWorkerThreads = numWorkerThreads;
    this.traceParameters = traceParameters;
    this.optimizationIntervalMs = optimizationIntervalMs;

    try {
      final Injector workerInjector = injector.forkInjector();
      workerInjector.bindVolatileInstance(EMDeleteExecutor.class, new WorkerRemover());
      workerInjector.bindVolatileParameter(EMIdentifier.class, WORKER_EM_IDENTIFIER);
      workerInjector.bindVolatileParameter(RangeSupport.class, Boolean.TRUE);
      this.workerEMWrapper = workerInjector.getInstance(EMWrapper.class);

      final Injector serverInjector = injector.forkInjector();
      serverInjector.bindVolatileInstance(EMDeleteExecutor.class, new ServerRemover());
      serverInjector.bindVolatileParameter(EMIdentifier.class, SERVER_EM_IDENTIFIER);
      serverInjector.bindVolatileParameter(RangeSupport.class, Boolean.FALSE);
      this.serverEMWrapper = serverInjector.getInstance(EMWrapper.class);
      this.emRoutingTableManager = serverInjector.getInstance(EMRoutingTableManager.class);
      this.psDriver = serverInjector.getInstance(PSDriver.class);
      this.psNetworkSetup = serverInjector.getInstance(PSNetworkSetup.class);

      this.jobStateMachine = initStateMachine();

      final Injector optimizerInjector = injector.forkInjector();
      optimizerInjector.bindVolatileParameter(ServerEM.class, serverEMWrapper.getInstance());
      optimizerInjector.bindVolatileParameter(WorkerEM.class, workerEMWrapper.getInstance());
      optimizerInjector.bindVolatileInstance(AsyncDolphinDriver.class, this);
      this.optimizationOrchestrator = optimizerInjector.getInstance(OptimizationOrchestrator.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  private StateMachine initStateMachine() {
    return StateMachine.newBuilder()
        .addState(RUNNING, "The job is running")
        .addState(CLOSING_WORKERS, "The job is started being closed, first closing the worker contexts")
        .addState(CLOSING_SERVERS, "Closing the server contexts")
        .addState(CLOSING_ROOTS, "Closing root contexts of both server and worker")
        .setInitialState(RUNNING)
        .addTransition(RUNNING, CLOSING_WORKERS,
            "The job is finished, time to close the worker contexts")
        .addTransition(CLOSING_WORKERS, CLOSING_SERVERS,
            "Worker contexts are finished, time to close the the server contexts")
        .addTransition(CLOSING_SERVERS, CLOSING_ROOTS,
            "Both worker and server contexts are finished, time to close their root contexts")
        .build();
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
      contextActiveHandlersForWorker.add(getSecondContextActiveHandlerForWorker());
      evaluatorManager.allocateEvaluators(dataLoadingService.getNumberOfPartitions(),
          evalAllocHandlerForWorker, contextActiveHandlersForWorker);

      // Register the driver to the Network.
      final Identifier driverId = identifierFactory.getNewInstance(driverIdStr);
      workerEMWrapper.getNetworkSetup().registerConnectionFactory(driverId);
      serverEMWrapper.getNetworkSetup().registerConnectionFactory(driverId);
      psNetworkSetup.registerConnectionFactory(driverId);

      optimizerExecutor.submit(new Callable<Void>() {
        private static final long INIT_DELAY = 10000;

        @Override
        public Void call() throws Exception {
          // TODO #538: check actual timing of system init
          Thread.sleep(INIT_DELAY);
          while (jobStateMachine.getCurrentState().equals(RUNNING)) {
            optimizationOrchestrator.run();
            Thread.sleep(optimizationIntervalMs);
          }
          return null;
        }
      });
    }
  }

  /**
   * Returns an EventHandler which submits the first context(DataLoading compute context) to server-side evaluator.
   */
  public EventHandler<AllocatedEvaluator> getEvalAllocHandlerForServer() {
    return new EventHandler<AllocatedEvaluator>() {
      @Override
      public void onNext(final AllocatedEvaluator allocatedEvaluator) {
        LOG.log(Level.FINE, "Submitting Compute Context to {0}", allocatedEvaluator.getId());
        allocatedEvaluators.put(allocatedEvaluator.getId(), allocatedEvaluator);

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
        allocatedEvaluators.put(allocatedEvaluator.getId(), allocatedEvaluator);

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
        rootContexts.put(activeContext.getId(), activeContext);

        final int serverIndex = Integer.parseInt(
            activeContext.getId().substring(dataLoadingService.getComputeContextIdPrefix().length()));
        final String contextId = SERVER_CONTEXT_ID + "-" + serverIndex;
        final Configuration contextConf = Configurations.merge(
            ContextConfiguration.CONF
                .set(ContextConfiguration.IDENTIFIER, contextId)
                .build(),
            psDriver.getServerContextConfiguration(),
            serverEMWrapper.getConf().getContextConfiguration());
        final Configuration serviceConf = Configurations.merge(
            psDriver.getServerServiceConfiguration(contextId),
            Tang.Factory.getTang().newConfigurationBuilder(
                serverEMWrapper.getConf().getServiceConfigurationWithoutNameResolver(contextId, initServerCount))
                .bindNamedParameter(AddedEval.class, String.valueOf(addedEval))
                .build());

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
            Configurations.merge(serviceConf, traceConf, paramConf, emServerClientConf));
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
        serverContexts.put(activeContext.getId(), activeContext);
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
        rootContexts.put(activeContext.getId(), activeContext);

        final int workerIndex = workerContextIndexCounter.getAndIncrement();

        final String contextId = WORKER_CONTEXT_ID + "-" + workerIndex;
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
            MetricsCollectionService.getServiceConfiguration());
        final Configuration traceConf = traceParameters.getConfiguration();

        final Configuration otherParamConf = Tang.Factory.getTang().newConfigurationBuilder()
              .bindNamedParameter(NumWorkerThreads.class, Integer.toString(numWorkerThreads))
              .build();

        activeContext.submitContextAndService(contextConf,
            Configurations.merge(serviceConf, traceConf, paramConf, otherParamConf, emWorkerClientConf));
      }
    };
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
  public EventHandler<ActiveContext> getSecondContextActiveHandlerForWorker() {
    return new EventHandler<ActiveContext>() {
      @Override
      public void onNext(final ActiveContext activeContext) {
        LOG.log(Level.INFO, "Worker-side ParameterWorker context - {0}", activeContext);
        workerContexts.put(activeContext.getId(), activeContext);

        final int workerIndex = Integer.parseInt(activeContext.getId().substring(WORKER_CONTEXT_ID.length() + 1));
        final Configuration taskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, AsyncWorkerTask.TASK_ID_PREFIX + "-" + workerIndex)
            .set(TaskConfiguration.TASK, AsyncWorkerTask.class)
            .set(TaskConfiguration.ON_CLOSE, AsyncWorkerTask.CloseEventHandler.class)
            .build();
        final Configuration emDataIdConf = Tang.Factory.getTang().newConfigurationBuilder()
            .bindImplementation(DataIdFactory.class, RoundRobinDataIdFactory.class).build();

        activeContext.submitTask(Configurations.merge(taskConf, workerConf, emDataIdConf));
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
    }
  }

  final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      LOG.log(Level.INFO, "FailedEvaluator: {0}", failedEvaluator);
    }
  }

  final class CompletedEvaluatorHandler implements EventHandler<CompletedEvaluator> {
    @Override
    public void onNext(final CompletedEvaluator completedEvaluator) {
      LOG.log(Level.INFO, "CompletedEvaluator: {0}", completedEvaluator);
    }
  }

  /**
   * Handler for FailedContext.
   * It treats failed context as a same way of handling closed context by {@link ClosedContextHandler}.
   */
  final class FailedContextHandler implements EventHandler<FailedContext> {
    @Override
    public void onNext(final FailedContext failedContext) {
      LOG.log(Level.INFO, "FailedContext: {0}", failedContext);
      final String contextId = failedContext.getId();

      final boolean closeParent = handleFinishedContext(contextId);

      // close its parent context to assure job to be shutdown anyway
      if (closeParent) {
        final Optional<ActiveContext> parentContext = failedContext.getParentContext();
        if (parentContext.isPresent()) {
          LOG.log(Level.WARNING, "Close a context {0} , which is a parent of the failed context: {1}",
              new Object[]{parentContext, failedContext});
          parentContext.get().close();
        }
      }
    }
  }

  /**
   * Handler for ClosedContext.
   * The contexts should be one of following:
   *  1) contexts closed in job shutdown phase started by {@link #checkShutdown()}
   *  2) contexts closed by EM's Delete procedure
   *
   * For a context out of upper cases, it closes a parent of the context, if exist,
   * to prevent job from holding unnecessary resources.
   * It is also for the job to be shut down in unexpected cases.
   */
  final class ClosedContextHandler implements EventHandler<ClosedContext> {
    @Override
    public void onNext(final ClosedContext closedContext) {
      LOG.log(Level.INFO, "ClosedContext: {0}", closedContext);
      final String contextId = closedContext.getId();

      final boolean closeParent = handleFinishedContext(contextId);

      // close its parent context to assure job to be shutdown anyway
      if (closeParent) {
        final ActiveContext parentContext = closedContext.getParentContext();
        if (parentContext != null) {
          LOG.log(Level.WARNING, "Close a context {0} , which is a parent of the closed context: {1}",
              new Object[]{parentContext, closedContext});
          parentContext.close();
        }
      }
    }
  }

  private synchronized void handleFinishedWorkerContext(final String contextId) {
    workerContexts.remove(contextId);

    final String jobState = jobStateMachine.getCurrentState();
    switch (jobState) {
    case RUNNING:
      LOG.log(Level.WARNING, "Worker context {0} is closed when the job is not in shutdown phase", contextId);
      break;
    case CLOSING_WORKERS:
      if (workerContexts.isEmpty()) { // all worker contexts are closed
        jobStateMachine.setState(CLOSING_SERVERS);

        LOG.info("Start closing server contexts");
        for (final ActiveContext serverContext : serverContexts.values()) {
          serverContext.close();
        }
      }
      break;
    case CLOSING_SERVERS:
      LOG.log(Level.WARNING, "Worker context {0} is closed after starting close of servers", contextId);
      break;
    case CLOSING_ROOTS:
      LOG.log(Level.WARNING, "Worker context {0} is closed after starting close of root contexts", contextId);
      break;
    default:
      throw new RuntimeException("Unexpected state");
    }
  }

  private synchronized void handleFinishedServerContext(final String contextId) {
    serverContexts.remove(contextId);

    final String jobState = jobStateMachine.getCurrentState();
    switch (jobState) {
    case RUNNING:
      LOG.log(Level.WARNING, "Server context {0} is closed when the job is not in shutdown phase", contextId);
      break;
    case CLOSING_WORKERS:
      LOG.log(Level.WARNING, "Server context {0} is closed after starting close of root contexts", contextId);
      break;
    case CLOSING_SERVERS:
      if (serverContexts.isEmpty()) { // all server contexts are closed
        jobStateMachine.setState(CLOSING_ROOTS);

        LOG.info("Start closing root contexts");
        for (final ActiveContext rootContext : rootContexts.values()) {
          rootContext.close();
        }
      }
      break;
    case CLOSING_ROOTS:
      LOG.log(Level.WARNING, "Server context {0} is closed after starting close of root contexts", contextId);
      break;
    default:
      throw new RuntimeException("Unexpected state");
    }
  }

  /**
   * Handles contexts failed or closed.
   * If a context is one of contexts under tracking, it handles the context properly.
   * @param contextId an identifier of the context
   * @return true if the parent of the context needs to be shut down
   */
  private boolean handleFinishedContext(final String contextId) {
    boolean needToCloseParent = false;

    // shutdown remaining contexts
    if (workerContexts.containsKey(contextId)) {
      handleFinishedWorkerContext(contextId);
    } else if (serverContexts.containsKey(contextId)) {
      handleFinishedServerContext(contextId);
    } else if (rootContexts.remove(contextId) != null) {
      // for tracking contexts
      LOG.log(Level.INFO, "Root context {0} is closed. Its evaluator will be released soon.", contextId);
    } else if (deletedWorkerContextIds.remove(contextId) || deletedServerContextIds.remove(contextId)) {
      LOG.log(Level.INFO, "The context {0} is closed by EM's Delete.", contextId);
      needToCloseParent = true;
    } else {
      LOG.log(Level.WARNING, "Untracked context: {0}", contextId);
      needToCloseParent = true;
    }

    return needToCloseParent;
  }

  final class FailedTaskHandler implements EventHandler<FailedTask> {
    @Override
    public void onNext(final FailedTask failedTask) {
      LOG.log(Level.INFO, "FailedTask: {0}", failedTask);

      // Close the context promptly when the task is complete by EM's Delete to make it reusable for EM's Add
      // On the contrary, do not close the context, because these evaluators still have valid data
      // which can be accessed by other running evaluators
      if (failedTask.getActiveContext().isPresent()) {
        final ActiveContext context = failedTask.getActiveContext().get();
        if (deletedWorkerContextIds.contains(context.getId())) {
          context.close(); // we can reuse this evaluator by submitting new context
        }
      }

      contextIdToWorkerTasks.remove(failedTask.getActiveContext().get().getId());
      checkShutdown(); // we may resubmit task in future
    }
  }

  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask completedTask) {
      LOG.log(Level.INFO, "CompletedTask: {0}", completedTask);

      // Close the context promptly when the task is complete by EM's Delete to make it reusable for EM's Add
      // On the contrary, do not close the context, because these evaluators still have valid data
      // which can be accessed by other running evaluators
      final ActiveContext context = completedTask.getActiveContext();
      if (deletedWorkerContextIds.contains(context.getId())) {
        context.close(); // we can reuse this evaluator by submitting new context
      }

      contextIdToWorkerTasks.remove(completedTask.getActiveContext().getId());
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

    if (jobStateMachine.compareAndSetState(RUNNING, CLOSING_WORKERS)) {
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
              LOG.log(Level.FINEST, "Interrupted while waiting for plan finish", e);
            }
          }

          LOG.log(Level.INFO, "Shutdown worker contexts");
          // Since it is not guaranteed that the messages from workers are completely received and processed,
          // the servers may lose some updates.
          for (final ActiveContext workerContext : workerContexts.values()) {
            workerContext.close();
          }

          // wait for worker contexts to be closed
          try {
            Thread.sleep(CONTEXT_CLOSE_GRACE_PERIOD_MS);
          } catch (InterruptedException e) {
            LOG.log(Level.FINEST, "Interrupted while waiting for close of worker contexts", e);
          }

          if (jobStateMachine.compareAndSetState(CLOSING_WORKERS, CLOSING_SERVERS)) {
            LOG.log(Level.WARNING, "Some workers do not respond to close messages within time." +
                " Start closing server contexts.");

            // ignore remaining worker contexts and close server contexts
            for (final ActiveContext serverContext : serverContexts.values()) {
              serverContext.close();
            }
          }

          try {
            Thread.sleep(CONTEXT_CLOSE_GRACE_PERIOD_MS);
          } catch (InterruptedException e) {
            LOG.log(Level.FINEST, "Interrupted while waiting for close of server contexts", e);
          }

          if (jobStateMachine.compareAndSetState(CLOSING_SERVERS, CLOSING_ROOTS)) {
            LOG.log(Level.WARNING, "Some servers do not respond to close messages within time." +
                " Start closing root contexts.");

            // ignore remaining server contexts and close root contexts
            for (final ActiveContext rootContext : rootContexts.values()) {
              rootContext.close();
            }
          }

          try {
            Thread.sleep(CONTEXT_CLOSE_GRACE_PERIOD_MS);
          } catch (InterruptedException e) {
            LOG.log(Level.FINEST, "Interrupted while waiting for close of root contexts", e);
          }

          LOG.log(Level.INFO, "Some root contexts do not respond to close message. Close evaluators directly.");
          // If I am still alive, then kill evaluators directly via AllocatedEvaluator.close(), which uses RM
          for (final AllocatedEvaluator allocatedEvaluator : allocatedEvaluators.values()) {
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
    public boolean execute(final String activeContextId, final EventHandler<AvroElasticMemoryMessage> callback) {
      final ActiveContext activeContext = serverContexts.remove(activeContextId);
      final boolean isSuccess;
      if (activeContext == null) {
        LOG.log(Level.WARNING,
            "Trying to remove active context {0}, which is not found", activeContextId);
        isSuccess = false;
      } else {
        deletedServerContextIds.add(activeContextId);

        activeContext.close();
        LOG.log(Level.FINE, "Server has been deleted successfully. Remaining Servers: {0}", serverContexts.size());
        isSuccess = true;
      }
      sendCallback(activeContextId, callback, isSuccess);
      return isSuccess;
    }
  }

  /**
   * Deletes Workers by closing active contexts that the Workers are run on.
   * Should be package private (not private), since Tang complains Explicit constructor in @Unit class.
   */
  final class WorkerRemover implements EMDeleteExecutor {
    @Override
    public boolean execute(final String activeContextId, final EventHandler<AvroElasticMemoryMessage> callback) {
      final ActiveContext activeContext = workerContexts.remove(activeContextId);
      final boolean isSuccess;
      if (activeContext == null) {
        LOG.log(Level.WARNING,
            "Trying to remove active context {0}, which is not found", activeContextId);
        isSuccess = false;
      } else {
        deletedWorkerContextIds.add(activeContextId);

        final RunningTask runningTask = contextIdToWorkerTasks.get(activeContextId);
        runningTask.close();

        // context will be closed in ClosedTaskHandler
        LOG.log(Level.FINE, "Worker has been deleted successfully. Remaining workers: {0}", workerContexts.size());
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
                            final EventHandler<AvroElasticMemoryMessage> callback,
                            final boolean isSuccess) {
    callback.onNext(AvroElasticMemoryMessage.newBuilder()
        .setType(Type.ResultMsg)
        .setResultMsg(ResultMsg.newBuilder().setResult(isSuccess ? Result.SUCCESS : Result.FAILURE).build())
        .setSrcId(contextId)
        .setDestId("")
        .build());
  }
}
