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
package edu.snu.cay.async;

import edu.snu.cay.async.AsyncDolphinLauncher.*;
import edu.snu.cay.async.optimizer.OptimizationOrchestrator;
import edu.snu.cay.async.optimizer.ServerEM;
import edu.snu.cay.async.optimizer.WorkerEM;
import edu.snu.cay.async.metric.MetricsCollectionService;
import edu.snu.cay.common.aggregation.driver.AggregationManager;
import edu.snu.cay.common.param.Parameters.NumWorkerThreads;
import edu.snu.cay.services.em.avro.AvroElasticMemoryMessage;
import edu.snu.cay.services.em.avro.Result;
import edu.snu.cay.services.em.avro.ResultMsg;
import edu.snu.cay.services.em.avro.Type;
import edu.snu.cay.services.em.common.parameters.MemoryStoreId;
import edu.snu.cay.services.em.common.parameters.RangeSupport;
import edu.snu.cay.services.em.driver.EMWrapper;
import edu.snu.cay.services.em.driver.api.EMDeleteExecutor;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.impl.RoundRobinDataIdFactory;
import edu.snu.cay.services.em.ns.parameters.EMIdentifier;
import edu.snu.cay.services.em.optimizer.conf.OptimizerClass;
import edu.snu.cay.services.em.plan.conf.PlanExecutorClass;
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
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.data.loading.api.DataLoadingService;
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
import java.util.ArrayList;
import java.util.List;
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
  private static final String WORKER_CONTEXT = "WorkerContext";
  private static final String SERVER_CONTEXT = "ServerContext";
  private static final String WORKER_EM_IDENTIFIER = "WorkerEM";
  private static final String SERVER_EM_IDENTIFIER = "ServerEM";
  private static final long OPTIMIZATION_INTERVAL_MS = 1000;
  private static final long OPTIMIZATION_INITIAL_DELAY_MS = 5000;

  private volatile boolean isComplete = false;

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
   * The initial number of worker-side evaluators.
   */
  private final int initWorkerCount;

  /**
   * The initial number of server-side evaluators.
   */
  private final int initServerCount;

  /**
   * Exchange messages between the driver and evaluators.
   */
  private final AggregationManager aggregationManager;

  /**
   * Accessor for parameter server service.
   */
  private final ParameterServerDriver psDriver;

  /**
   * Accessor for configurations of elastic memory service.
   */

  /**
   * Class providing a configuration for HTrace.
   */
  private final HTraceParameters traceParameters;

  /**
   * Number of worker-side evaluators that have successfully passed {@link ActiveContextHandler}.
   */
  private final AtomicInteger runningWorkerContextCount;

  /**
   * Number of server-side evaluators that have successfully passed {@link ActiveContextHandler}.
   */
  private final AtomicInteger runningServerContextCount;

  private final ConcurrentMap<String, ActiveContext> activeContexs = new ConcurrentHashMap<>();

  /**
   * Number of evaluators that have completed or failed.
   */
  private final AtomicInteger completedOrFailedEvalCount;

  /**
   * Configuration that should be passed to each {@link AsyncWorkerTask}.
   */
  private final Configuration workerConf;
  private final Configuration paramConf;

  /**
   * Queue of activeContext objects of the evaluators housing the parameter server.
   */
  private ConcurrentLinkedQueue<ActiveContext> serverContexts;

  /**
   * Queue of activeContext objects of the worker-side evaluators waiting for driver to close.
   */
  private ConcurrentLinkedQueue<ActiveContext> workerContextsToClose;

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
                             @Parameter(NumServers.class) final int numServers,
                             final ConfigurationSerializer configurationSerializer,
                             @Parameter(NumWorkerThreads.class) final int numWorkerThreads,
                             @Parameter(PlanExecutorClass.class) final String planExecutorClass,
                             @Parameter(OptimizerClass.class) final String optimizerClass,
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
    this.runningWorkerContextCount = new AtomicInteger(0);
    this.runningServerContextCount = new AtomicInteger(0);
    this.completedOrFailedEvalCount = new AtomicInteger(0);
    this.serverContexts = new ConcurrentLinkedQueue<>();
    this.workerContextsToClose = new ConcurrentLinkedQueue<>();
    this.workerConf = configurationSerializer.fromString(serializedWorkerConf);
    this.paramConf = configurationSerializer.fromString(serializedParamConf);
    this.numWorkerThreads = numWorkerThreads;
    this.traceParameters = traceParameters;

    try {
      final Injector workerInjector = injector.forkInjector();
      workerInjector.bindVolatileParameter(EMIdentifier.class, WORKER_EM_IDENTIFIER);
      workerInjector.bindVolatileParameter(RangeSupport.class, Boolean.TRUE);
      this.workerEMWrapper = workerInjector.getInstance(EMWrapper.class);

      final Injector serverInjector = injector.forkInjector();
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
      contextActiveHandlersForServer.add(getFirstContextActiveHandlerForServer());
      contextActiveHandlersForServer.add(getSecondContextActiveHandlerForServer());
      evaluatorManager.allocateEvaluators(initServerCount, evalAllocHandlerForServer, contextActiveHandlersForServer);

      final EventHandler<AllocatedEvaluator> evalAllocHandlerForWorker = getEvalAllocHandlerForWorker();
      final List<EventHandler<ActiveContext>> contextActiveHandlersForWorker = new ArrayList<>(2);
      contextActiveHandlersForWorker.add(getFirstContextActiveHandlerForWorker());
      contextActiveHandlersForWorker.add(getSecondContextActiveHandlerForWorker());
      evaluatorManager.allocateEvaluators(dataLoadingService.getNumberOfPartitions(),
          evalAllocHandlerForWorker, contextActiveHandlersForWorker);

      // Register the driver to the Network.
      final Identifier driverId = identifierFactory.getNewInstance(driverIdStr);
      workerEMWrapper.getNetworkSetup().registerConnectionFactory(driverId);
      serverEMWrapper.getNetworkSetup().registerConnectionFactory(driverId);
      psNetworkSetup.registerConnectionFactory(driverId);

      optimizerExecutor.submit(new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          Thread.sleep(OPTIMIZATION_INITIAL_DELAY_MS);
          while (!isComplete) {
            Thread.sleep(OPTIMIZATION_INTERVAL_MS);
            optimizationOrchestrator.run();
          }
          return null;
        }
      });
    }
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
      activeContexs.putIfAbsent(activeContext.getId(), activeContext);
    }
  }

  final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      checkShutdown();
    }
  }

  final class FailedContextHandler implements EventHandler<FailedContext> {
    @Override
    public void onNext(final FailedContext failedContext) {
      checkShutdown();
    }
  }

  final class FailedTaskHandler implements EventHandler<FailedTask> {
    @Override
    public void onNext(final FailedTask failedTask) {
      if (failedTask.getActiveContext().isPresent()) {
        workerContextsToClose.add(failedTask.getActiveContext().get());
      } else {
        LOG.log(Level.WARNING, "FailedTask {0} has no parent context", failedTask);
      }

      checkShutdown();
    }
  }

  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask completedTask) {
      workerContextsToClose.add(completedTask.getActiveContext());
      checkShutdown();
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
        final int serverIndex = runningServerContextCount.getAndIncrement();
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
  public EventHandler<ActiveContext> getFirstContextActiveHandlerForServer() {
    return new EventHandler<ActiveContext>() {
      @Override
      public void onNext(final ActiveContext activeContext) {
        LOG.log(Level.INFO, "Server-side Compute context - {0}", activeContext);
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
            psDriver.getServerServiceConfiguration(),
            serverEMWrapper.getConf().getServiceConfigurationWithoutNameResolver(contextId, initServerCount));

        final Injector serviceInjector = Tang.Factory.getTang().newInjector(serviceConf);
        try {
          // to synchronize EM's MemoryStore id and PS's Network endpoint.
          final int memoryStoreId = serviceInjector.getNamedInstance(MemoryStoreId.class);
          final String endpointId = serviceInjector.getNamedInstance(EndpointId.class);
          emRoutingTableManager.register(memoryStoreId, endpointId);
        } catch (final InjectionException e) {
          throw new RuntimeException(e);
        }

        final Configuration traceConf = traceParameters.getConfiguration();

        activeContext.submitContextAndService(contextConf,
            Configurations.merge(serviceConf, traceConf, paramConf));
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
        registerActiveContextForServer(activeContext);
      }
    };
  }

  /**
   * Registers the Active contexts. Should be called when Evaluators are added by ElasticMemory.
   * (See {@link edu.snu.cay.async.optimizer.AsyncDolphinPlanExecutor})
   */
  public void registerActiveContextForServer(final ActiveContext activeContext) {
    LOG.log(Level.INFO, "Server-side ParameterServer context - {0}", activeContext);
    completedOrFailedEvalCount.incrementAndGet();
    // although this evaluator is not 'completed' yet,
    // we add it beforehand so that it closes if all workers finish
    serverContexts.add(activeContext);
  }

  /**
   * Returns an EventHandler which submits the second context(worker context) to worker-side evaluator.
   */
  public EventHandler<ActiveContext> getFirstContextActiveHandlerForWorker() {
    return new EventHandler<ActiveContext>() {
      @Override
      public void onNext(final ActiveContext activeContext) {
        LOG.log(Level.INFO, "Worker-side DataLoad context - {0}", activeContext);
        final int workerIndex = runningWorkerContextCount.getAndIncrement();
        final String contextId = WORKER_CONTEXT + "-" + workerIndex;
        final Configuration contextConf = Configurations.merge(
            ContextConfiguration.CONF
                .set(ContextConfiguration.IDENTIFIER, contextId)
                .build(),
            psDriver.getContextConfiguration(),
            workerEMWrapper.getConf().getContextConfiguration(),
            aggregationManager.getContextConfiguration());
        final Configuration serviceConf = Configurations.merge(
            psDriver.getWorkerServiceConfiguration(),
            workerEMWrapper.getConf().getServiceConfigurationWithoutNameResolver(contextId, initWorkerCount),
            aggregationManager.getServiceConfigurationWithoutNameResolver(),
            MetricsCollectionService.getServiceConfiguration());
        final Configuration traceConf = traceParameters.getConfiguration();
        final Configuration otherParamConf = Tang.Factory.getTang().newConfigurationBuilder()
            .bindNamedParameter(NumWorkerThreads.class, Integer.toString(numWorkerThreads))
            .build();

        activeContext.submitContextAndService(contextConf,
            Configurations.merge(serviceConf, traceConf, paramConf, otherParamConf));
      }
    };
  }

  /**
   * Returns an EventHandler which submits worker task to worker-side evaluator.
   */
  public EventHandler<ActiveContext> getSecondContextActiveHandlerForWorker() {
    return new EventHandler<ActiveContext>() {
      @Override
      public void onNext(final ActiveContext activeContext) {
        LOG.log(Level.INFO, "Worker-side ParameterWorker context - {0}", activeContext);
        final int workerIndex = Integer.parseInt(activeContext.getId().substring(WORKER_CONTEXT.length() + 1));
        final Configuration taskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, AsyncWorkerTask.TASK_ID_PREFIX + "-" + workerIndex)
            .set(TaskConfiguration.TASK, AsyncWorkerTask.class)
            .build();
        final Configuration emDataIdConf = Tang.Factory.getTang().newConfigurationBuilder()
            .bindImplementation(DataIdFactory.class, RoundRobinDataIdFactory.class).build();

        activeContext.submitTask(Configurations.merge(taskConf, workerConf, emDataIdConf));
      }
    };
  }

  private void checkShutdown() {
    if (completedOrFailedEvalCount.incrementAndGet() == initWorkerCount + initServerCount) {
      // Since it is not guaranteed that the messages from workers are completely received and processed,
      // the servers may lose some updates.
      for (final ActiveContext serverContext : serverContexts) {
        serverContext.close();
      }
      for (final ActiveContext workerContext : workerContextsToClose) {
        workerContext.close();
      }
      isComplete = true;
    }
  }

  /**
   * EMDeleteExecutor implementation for Dolphin async.
   */
  final class TaskRemover implements EMDeleteExecutor {
    @Override
    public void execute(final String activeContextId, final EventHandler<AvroElasticMemoryMessage> callback) {
      final ActiveContext activeContext = activeContexs.get(activeContextId);
      if (activeContext == null) {
        // Given active context should have a runningTask in a normal case, because our job is paused.
        // Evaluator without corresponding runningTask implies error.
        LOG.log(Level.WARNING,
            "Trying to remove running task on active context {0}. Cannot find running task on it", activeContextId);
      } else {
        // TODO #205: Reconsider using of Avro message in EM's callback
        activeContext.close();
        callback.onNext(AvroElasticMemoryMessage.newBuilder()
            .setType(Type.ResultMsg)
            .setResultMsg(ResultMsg.newBuilder().setResult(Result.SUCCESS).build())
            .setSrcId(activeContextId)
            .setDestId("")
            .build());
      }
    }
  }
}
