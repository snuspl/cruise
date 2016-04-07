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
import edu.snu.cay.common.aggregation.driver.AggregationManager;
import edu.snu.cay.common.param.Parameters.NumWorkerThreads;
import edu.snu.cay.services.em.driver.ElasticMemoryConfiguration;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.impl.BaseCounterDataIdFactory;
import edu.snu.cay.services.evalmanager.api.EvaluatorManager;
import edu.snu.cay.services.ps.common.partitioned.parameters.NumServers;
import edu.snu.cay.services.ps.driver.ParameterServerDriver;
import edu.snu.cay.utils.trace.HTrace;
import edu.snu.cay.utils.trace.HTraceParameters;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.data.loading.api.DataLoadingService;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver code for {@code dolphin-async} applications.
 */
@DriverSide
@Unit
final class AsyncDolphinDriver {
  private static final Logger LOG = Logger.getLogger(AsyncDolphinDriver.class.getName());
  private static final String WORKER_CONTEXT = "WorkerContext";
  private static final String SERVER_CONTEXT = "ServerContext";

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
  private final ElasticMemoryConfiguration emConf;

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

  @Inject
  private AsyncDolphinDriver(final EvaluatorManager evaluatorManager,
                             final DataLoadingService dataLoadingService,
                             final ParameterServerDriver psDriver,
                             final ElasticMemoryConfiguration emConf,
                             final AggregationManager aggregationManager,
                             @Parameter(SerializedWorkerConfiguration.class) final String serializedWorkerConf,
                             @Parameter(SerializedParameterConfiguration.class) final String serializedParamConf,
                             @Parameter(NumServers.class) final int numServers,
                             final ConfigurationSerializer configurationSerializer,
                             @Parameter(NumWorkerThreads.class) final int numWorkerThreads,
                             final HTraceParameters traceParameters,
                             final HTrace hTrace) throws IOException {
    hTrace.initialize();
    this.evaluatorManager = evaluatorManager;
    this.dataLoadingService = dataLoadingService;
    this.initWorkerCount = dataLoadingService.getNumberOfPartitions();
    this.initServerCount = numServers;
    this.psDriver = psDriver;
    this.emConf = emConf;
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
    }

    /**
     * Returns an EventHandler which submits the first context(DataLoading compute context) to server-side evaluator.
     */
    private EventHandler<AllocatedEvaluator> getEvalAllocHandlerForServer() {
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
    private EventHandler<AllocatedEvaluator> getEvalAllocHandlerForWorker() {
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
    private EventHandler<ActiveContext> getFirstContextActiveHandlerForServer() {
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
              psDriver.getContextConfiguration(), emConf.getContextConfiguration());
          final Configuration serviceConf = Configurations.merge(psDriver.getServerServiceConfiguration(),
              emConf.getServiceConfigurationWithoutNameResolver(contextId));
          final Configuration traceConf = traceParameters.getConfiguration();
          final Configuration emDataIdConf = Tang.Factory.getTang().newConfigurationBuilder()
              .bindImplementation(DataIdFactory.class, BaseCounterDataIdFactory.class).build();

          activeContext.submitContextAndService(contextConf,
              Configurations.merge(serviceConf, traceConf, paramConf, emDataIdConf));
        }
      };
    }

    /**
     * Returns an EventHandler which finishes server-side evaluator setup.
     */
    private EventHandler<ActiveContext> getSecondContextActiveHandlerForServer() {
      return new EventHandler<ActiveContext>() {
        @Override
        public void onNext(final ActiveContext activeContext) {
          LOG.log(Level.INFO, "Server-side ParameterServer context - {0}", activeContext);
          completedOrFailedEvalCount.incrementAndGet();
          // although this evaluator is not 'completed' yet,
          // we add it beforehand so that it closes if all workers finish
          serverContexts.add(activeContext);
        }
      };
    }

    /**
     * Returns an EventHandler which submits the second context(worker context) to worker-side evaluator.
     */
    private EventHandler<ActiveContext> getFirstContextActiveHandlerForWorker() {
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
              psDriver.getContextConfiguration(), emConf.getContextConfiguration(),
              aggregationManager.getContextConfiguration());
          final Configuration serviceConf = Configurations.merge(
              psDriver.getWorkerServiceConfiguration(), emConf.getServiceConfigurationWithoutNameResolver(contextId),
              aggregationManager.getServiceConfigurationWithoutNameResolver());
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
    private EventHandler<ActiveContext> getSecondContextActiveHandlerForWorker() {
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
              .bindImplementation(DataIdFactory.class, BaseCounterDataIdFactory.class).build();

          activeContext.submitTask(Configurations.merge(taskConf, workerConf, emDataIdConf));
        }
      };
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
    }
  }
}
