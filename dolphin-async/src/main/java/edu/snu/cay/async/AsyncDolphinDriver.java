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
import edu.snu.cay.services.dataloader.DataLoader;
import edu.snu.cay.services.ps.driver.ParameterServerDriver;
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
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.io.IOException;
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
   * Customized REEF DataLoader.
   * Sends evaluator allocation requests for data loading service.
   */
  private final DataLoader dataLoader;

  /**
   * Accessor for parameter server service.
   */
  private final ParameterServerDriver psDriver;

  /**
   * Number of worker-side evaluators that have successfully passed {@link ActiveContextHandler}.
   */
  private final AtomicInteger runningWorkerContextCount;

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
   * ActiveContext object of the evaluator housing the parameter server.
   */
  private ActiveContext serverContext;

  @Inject
  private AsyncDolphinDriver(final DataLoadingService dataLoadingService,
                             final DataLoader dataLoader,
                             final ParameterServerDriver psDriver,
                             @Parameter(SerializedWorkerConfiguration.class) final String serializedWorkerConf,
                             @Parameter(SerializedParameterConfiguration.class) final String serializedParamConf,
                             final ConfigurationSerializer configurationSerializer) throws IOException {
    this.dataLoadingService = dataLoadingService;
    this.initWorkerCount = dataLoadingService.getNumberOfPartitions();
    this.dataLoader = dataLoader;
    this.psDriver = psDriver;
    this.runningWorkerContextCount = new AtomicInteger(0);
    this.completedOrFailedEvalCount = new AtomicInteger(0);
    this.workerConf = configurationSerializer.fromString(serializedWorkerConf);
    this.paramConf = configurationSerializer.fromString(serializedParamConf);
  }

  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "StartTime: {0}", startTime);
      dataLoader.releaseResourceRequestGate();
    }
  }

  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "AllocatedEvaluator: {0}", allocatedEvaluator);
      // currently we only have data loading service,
      // but later we should check if this evaluator was actually requested by the service
      dataLoader.handleDataLoadingEvalAlloc(allocatedEvaluator);
    }
  }

  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext activeContext) {
      LOG.log(Level.INFO, "ActiveContext: {0}", activeContext);

      // Case 1: One context on worker-side evaluator
      if (dataLoadingService.isDataLoadedContext(activeContext)) {
        LOG.log(Level.INFO, "Worker-side DataLoad context - {0}", activeContext);
        final int workerIndex = runningWorkerContextCount.getAndIncrement();
        final Configuration contextConf = Configurations.merge(
            ContextConfiguration.CONF
                .set(ContextConfiguration.IDENTIFIER, WORKER_CONTEXT + "-" + workerIndex)
                .build(),
            psDriver.getContextConfiguration());
        final Configuration serviceConf = psDriver.getWorkerServiceConfiguration();

        activeContext.submitContextAndService(contextConf, serviceConf);

      // TODO #361: Adjust to use multiple servers for multi-node parameter server
      // Case 2: One context on server-side evaluator
      } else if (dataLoadingService.isComputeContext(activeContext)) {
        LOG.log(Level.INFO, "Server-side Compute context - {0}", activeContext);
        final Configuration contextConf = Configurations.merge(
            ContextConfiguration.CONF
                .set(ContextConfiguration.IDENTIFIER, SERVER_CONTEXT)
                .build(),
            psDriver.getContextConfiguration());
        final Configuration serviceConf = psDriver.getServerServiceConfiguration();

        activeContext.submitContextAndService(contextConf, Configurations.merge(serviceConf, paramConf));

      // Case 3: Two contexts on worker-side evaluator
      } else if (activeContext.getId().startsWith(WORKER_CONTEXT)) {
        LOG.log(Level.INFO, "Worker-side ParameterWorker context - {0}", activeContext);
        final int workerIndex = Integer.parseInt(activeContext.getId().substring(WORKER_CONTEXT.length() + 1));
        final Configuration taskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, AsyncWorkerTask.TASK_ID_PREFIX + "-" + workerIndex)
            .set(TaskConfiguration.TASK, AsyncWorkerTask.class)
            .build();

        activeContext.submitTask(Configurations.merge(taskConf, workerConf, paramConf));

      // Case 4: Two contexts on server-side evaluator
      } else if (activeContext.getId().equals(SERVER_CONTEXT)) {
        LOG.log(Level.INFO, "Server-side ParameterServer context - {0}", activeContext);
        completedOrFailedEvalCount.incrementAndGet();
        // although this evaluator is not 'completed' yet, we add it beforehand so that it closes if all workers finish
        serverContext = activeContext;

      } else {
        LOG.log(Level.WARNING, "Unexpected context - {0}", activeContext);
        activeContext.close();
      }
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
        failedTask.getActiveContext().get().close();
      } else {
        LOG.log(Level.WARNING, "FailedTask {0} has no parent context", failedTask);
      }

      checkShutdown();
    }
  }

  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask completedTask) {
      completedTask.getActiveContext().close();
      checkShutdown();
    }
  }

  private void checkShutdown() {
    if (completedOrFailedEvalCount.incrementAndGet() == initWorkerCount + 1 && serverContext != null) {
      serverContext.close();
    }
  }
}
