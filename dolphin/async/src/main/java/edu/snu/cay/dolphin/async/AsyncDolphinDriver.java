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
import edu.snu.cay.services.ps.common.partitioned.parameters.NumServers;
import edu.snu.cay.services.ps.driver.ParameterServerDriver;
import edu.snu.cay.services.ps.driver.impl.EMRoutingTableManager;
import edu.snu.cay.services.ps.ns.EndpointId;
import edu.snu.cay.services.ps.ns.PSNetworkSetup;
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
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private static final String WORKER_CONTEXT = "WorkerContext";
  private static final String SERVER_CONTEXT = "ServerContext";
  private static final String WORKER_EM_IDENTIFIER = "WorkerEM";
  private static final String SERVER_EM_IDENTIFIER = "ServerEM";

  /**
   * Interval between trials to shutdown contexts to make sure
   * no optimization plan is executing.
   */
  private static final long SHUTDOWN_TRIAL_INTERVAL_MS = 3000;

  /**
   * Flag indicating whether all workers have finished their work or not.
   * When it becomes true, a thread starts to close all worker contexts and
   * the optimizer thread stops running.
   */
  private final AtomicBoolean isJobFinished = new AtomicBoolean(false);

  /**
   * Flag indicating a phase of job shutdown closing servers contexts, after all worker contexts are closed.
   */
  private final AtomicBoolean closingServerContexts = new AtomicBoolean(false);

  /**
   * Flag indicating a final phase of job shutdown closing root contexts of server and worker.
   */
  private final AtomicBoolean closingRootContexts = new AtomicBoolean(false);

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
  private final ParameterServerDriver psDriver;

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
   * Bookkeeping the activeContext objects of the root context of the server-side evaluators.
   */
  private final ConcurrentMap<String, ActiveContext> rootWorkerContexts = new ConcurrentHashMap<>();

  /**
   * Bookkeeping the activeContext objects of the root context of the worker-side evaluators.
   */
  private final ConcurrentMap<String, ActiveContext> rootServerContexts = new ConcurrentHashMap<>();

  /**
   * Bookkeeping the activeContext objects of the context housing the parameter server.
   */
  private final ConcurrentMap<String, ActiveContext> serverContexts = new ConcurrentHashMap<>();

  /**
   * Bookkeeping the activeContext objects of the context that the worker task is running on.
   */
  private final ConcurrentMap<String, ActiveContext> workerContexts = new ConcurrentHashMap<>();

  /**
   * Bookkeeping the running task of workers.
   */
  private final ConcurrentMap<String, RunningTask> contextIdToRunningWorkerTasks = new ConcurrentHashMap<>();

  /**
   * Bookkeeping of context id of workers deleted by EM's Delete.
   */
  private final Set<String> deletedWorkerContextIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

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
   * To establish connections between the Driver and PS Workers.
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
      this.psDriver = serverInjector.getInstance(ParameterServerDriver.class);
      this.psNetworkSetup = serverInjector.getInstance(PSNetworkSetup.class);

      final Injector optimizerInjector = injector.forkInjector();
      optimizerInjector.bindVolatileParameter(ServerEM.class, serverEMWrapper.getInstance());
      optimizerInjector.bindVolatileParameter(WorkerEM.class, workerEMWrapper.getInstance());
      optimizerInjector.bindVolatileInstance(AsyncDolphinDriver.class, this);
      this.optimizationOrchestrator = optimizerInjector.getInstance(OptimizationOrchestrator.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
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
          while (!isJobFinished.get()) {
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
        rootServerContexts.putIfAbsent(activeContext.getId(), activeContext);

        final int serverIndex = Integer.parseInt(
            activeContext.getId().substring(dataLoadingService.getComputeContextIdPrefix().length()));
        final String contextId = SERVER_CONTEXT + "-" + serverIndex;
        final Configuration contextConf = Configurations.merge(
            ContextConfiguration.CONF
                .set(ContextConfiguration.IDENTIFIER, contextId)
                .build(),
            psDriver.getContextConfiguration(),
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
        serverContexts.putIfAbsent(activeContext.getId(), activeContext);
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
        rootWorkerContexts.putIfAbsent(activeContext.getId(), activeContext);

        final int workerIndex = workerContextIndexCounter.getAndIncrement();

        final String contextId = WORKER_CONTEXT + "-" + workerIndex;
        final Configuration contextConf = Configurations.merge(
            ContextConfiguration.CONF
                .set(ContextConfiguration.IDENTIFIER, contextId)
                .build(),
            psDriver.getContextConfiguration(),
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
        workerContexts.putIfAbsent(activeContext.getId(), activeContext);

        final int workerIndex = Integer.parseInt(activeContext.getId().substring(WORKER_CONTEXT.length() + 1));
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
      contextIdToRunningWorkerTasks.putIfAbsent(runningTask.getActiveContext().getId(), runningTask);
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

  final class FailedContextHandler implements EventHandler<FailedContext> {
    @Override
    public void onNext(final FailedContext failedContext) {
      LOG.log(Level.INFO, "FailedContext: {0}", failedContext);
    }
  }

  final class ClosedContextHandler implements EventHandler<ClosedContext> {
    @Override
    public void onNext(final ClosedContext closedContext) {
      LOG.log(Level.INFO, "ClosedContext: {0}", closedContext);
      final String contextId = closedContext.getId();

      if (workerContexts.containsKey(contextId)) {
        workerContexts.remove(contextId);
        if (workerContexts.isEmpty()) { // all worker contexts are closed
          if (closingServerContexts.compareAndSet(false, true)) {
            LOG.info("Start closing server contexts");
            for (final ActiveContext serverContext : serverContexts.values()) {
              serverContext.close();
            }
          } else {
            LOG.log(Level.WARNING, "Errors in tracking worker context: {0}", closedContext);
          }
        }
      } else if (serverContexts.containsKey(contextId)) {
        serverContexts.remove(contextId);
        if (serverContexts.isEmpty()) {
          if (closingRootContexts.compareAndSet(false, true)) {
            LOG.info("Start closing root contexts");
            for (final ActiveContext rootWorkerContext : rootWorkerContexts.values()) {
              rootWorkerContext.close();
            }
            for (final ActiveContext rootServerContext : rootServerContexts.values()) {
              rootServerContext.close();
            }
          } else {
            LOG.log(Level.WARNING, "Errors in tracking server context: {0}", closedContext);
          }
        }
      } else {
        LOG.log(Level.WARNING, "Untracked context: {0}", closedContext);
      }
    }
  }

  final class FailedTaskHandler implements EventHandler<FailedTask> {
    @Override
    public void onNext(final FailedTask failedTask) {
      LOG.log(Level.INFO, "FailedTask: {0}", failedTask);

      contextIdToRunningWorkerTasks.remove(failedTask.getActiveContext().get().getId());
      if (contextIdToRunningWorkerTasks.isEmpty()) {
        shutdown();
      }
    }
  }

  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask completedTask) {
      LOG.log(Level.INFO, "CompletedTask: {0}", completedTask);

      // I'm not sure whether tasks closed by closing context generate this event or not
      if (deletedWorkerContextIds.remove(completedTask.getActiveContext().getId())) {
        // Close the resource promptly when the task is complete by EM's Delete to make it reusable for EM's Add
        final ActiveContext context = completedTask.getActiveContext();
        context.close(); // we can reuse this evaluator by submitting context with EM service
      }

      // Currently we do not reuse evaluators that completely finish their work for EM's Add
      // It is because these evaluators still have valid data, which can be accessed by other running evaluators
      contextIdToRunningWorkerTasks.remove(completedTask.getActiveContext().getId());
      if (contextIdToRunningWorkerTasks.isEmpty()) {
        shutdown();
      }
    }
  }

  /**
   * Shutdown job, when all worker tasks are finished.
   * Close the worker contexts first and then server contexts,
   * since worker-server communication is driven by worker.
   * After completely closing PS contexts, close the root contexts (DataLoading).
   */
  private void shutdown() {
    LOG.log(Level.INFO, "shutdown");

    if (isJobFinished.compareAndSet(false, true)) {
      // FINISH JOB: close all worker contexts
      Executors.newSingleThreadExecutor().submit(new Runnable() {
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
        }
      });
    } else {
      LOG.log(Level.WARNING, "Job is already being shutdown. Driver fails to correctly track worker tasks.");
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
      final ActiveContext activeContext = workerContexts.get(activeContextId);
      final boolean isSuccess;
      if (activeContext == null) {
        LOG.log(Level.WARNING,
            "Trying to remove active context {0}, which is not found", activeContextId);
        isSuccess = false;
      } else {
        deletedWorkerContextIds.add(activeContextId);

        final RunningTask runningTask = contextIdToRunningWorkerTasks.remove(activeContextId);
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
