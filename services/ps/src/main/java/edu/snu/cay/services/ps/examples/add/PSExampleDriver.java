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
package edu.snu.cay.services.ps.examples.add;

import edu.snu.cay.services.evalmanager.api.EvaluatorManager;
import edu.snu.cay.services.ps.common.Constants;
import edu.snu.cay.services.ps.driver.impl.PSDriver;
import edu.snu.cay.services.ps.examples.add.parameters.NumKeys;
import edu.snu.cay.services.ps.examples.add.parameters.NumUpdates;
import edu.snu.cay.services.ps.examples.add.parameters.NumWorkers;
import edu.snu.cay.services.ps.examples.add.parameters.StartKey;
import edu.snu.cay.services.ps.common.parameters.NumServers;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The ParameterServer Example Driver.
 * We launch Evaluators for Workers and Parameter Servers on Start (currently only one PS).
 * These Evaluators are then given PS Context and Services, and UpdaterTasks are run.
 * When all UpdaterTasks are complete, a ValidatorTask is run.
 * When the ValidatorTask is complete, the Parameter Service is shutdown;
 * the Driver will shutdown because it is idle.
 */
@DriverSide
@Unit
public final class PSExampleDriver {
  private static final Logger LOG = Logger.getLogger(PSExampleDriver.class.getName());

  private static final String WORKER_CONTEXT_PREFIX = "Worker-Context-";
  private static final String UPDATER_TASK_PREFIX = "Updater-Task-";
  private static final String VALIDATOR_TASK_ID = "Validator-Task";

  private final PSDriver psDriver;
  private final EvaluatorManager evaluatorManager;

  private final int numServers;
  private final int numWorkers;
  private final int numUpdatesPerWorker;
  private final int startKey;
  private final int numKeys;

  private final AtomicInteger runningServerContextCount = new AtomicInteger(0);
  private final AtomicInteger runningWorkerContextCount = new AtomicInteger(0);

  private final AtomicInteger taskCompletedCounter = new AtomicInteger(0);

  private ConcurrentLinkedQueue<ActiveContext> psContexts = new ConcurrentLinkedQueue<>();
  private ConcurrentLinkedQueue<ActiveContext> workerContextsToClose = new ConcurrentLinkedQueue<>();

  /**
   * numUpdates is the _total_ number of updates to push.
   * This value is then divided by numWorkers and passed as numUpdatesPerWorker to the Tasks.
   *
   * @throws IllegalArgumentException numUpdates must be divisible by numWorkers and (numWorkers * numKeys).
   *                                  If not, the exception is thrown.
   *                                  This makes accounting of updates across Workers much simpler.
   */
  @Inject
  private PSExampleDriver(final PSDriver psDriver,
                          final EvaluatorManager evaluatorManager,
                          @Parameter(NumServers.class) final int numServers,
                          @Parameter(NumWorkers.class) final int numWorkers,
                          @Parameter(NumUpdates.class) final int numUpdates,
                          @Parameter(StartKey.class) final int startKey,
                          @Parameter(NumKeys.class) final int numKeys) {
    if (numUpdates % numWorkers != 0) {
      throw new IllegalArgumentException(
          String.format("numUpdates %d must be divisible by numWorkers %d", numUpdates, numWorkers));
    }
    if ((numUpdates / numWorkers) % numKeys != 0) {
      throw new IllegalArgumentException(
          String.format("numUpdates %d must be divisible by (numWorkers %d * numKeys %d)",
              numUpdates, numWorkers, numKeys));
    }

    this.psDriver = psDriver;
    this.evaluatorManager = evaluatorManager;
    this.numServers = numServers;
    this.numWorkers = numWorkers;
    this.numUpdatesPerWorker = numUpdates / numWorkers;
    this.startKey = startKey;
    this.numKeys = numKeys;
  }

  /**
   * Launch Evaluators for Workers and Parameter Servers.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {

      final EventHandler<AllocatedEvaluator> evalAllocHandlerForServer = getEvalAllocHandlerForServer();
      final List<EventHandler<ActiveContext>> contextActiveHandlersForServer = new ArrayList<>(1);
      contextActiveHandlersForServer.add(getContextActiveHandlerForServer());
      evaluatorManager.allocateEvaluators(numServers, evalAllocHandlerForServer, contextActiveHandlersForServer);

      final EventHandler<AllocatedEvaluator> evalAllocHandlerForWorker = getEvalAllocHandlerForWorker();
      evaluatorManager.allocateEvaluators(numWorkers, evalAllocHandlerForWorker,
          new ArrayList<EventHandler<ActiveContext>>());
    }
  }

  /**
   * Submit the ParameterServer Context and Service.
   */
  private EventHandler<AllocatedEvaluator> getEvalAllocHandlerForServer() {
    return new EventHandler<AllocatedEvaluator>() {
      @Override
      public void onNext(final AllocatedEvaluator allocatedEvaluator) {
        final int serverIndex = runningServerContextCount.getAndIncrement();
        final String contextId = Constants.SERVER_ID_PREFIX + serverIndex;

        final Configuration contextConf = Configurations.merge(
            ContextConfiguration.CONF
                .set(ContextConfiguration.IDENTIFIER,
                    contextId)
                .build(),
            psDriver.getServerContextConfiguration());

        final Configuration serviceConf = psDriver.getServerServiceConfiguration(contextId);

        allocatedEvaluator.submitContextAndService(contextConf, serviceConf);
      }
    };
  }

  /**
   * Save the Parameter Server's active Context.
   */
  private EventHandler<ActiveContext> getContextActiveHandlerForServer() {
    return new EventHandler<ActiveContext>() {
      @Override
      public void onNext(final ActiveContext activeContext) {
        psContexts.add(activeContext);
      }
    };
  }

  /**
   * Submit a Context and Service for the ParameterWorker, and run an UpdaterTask on top of that.
   */
  private EventHandler<AllocatedEvaluator> getEvalAllocHandlerForWorker() {
    return new EventHandler<AllocatedEvaluator>() {
      @Override
      public void onNext(final AllocatedEvaluator allocatedEvaluator) {
        final int workerIndex = runningWorkerContextCount.getAndIncrement();
        final String contextId = WORKER_CONTEXT_PREFIX + workerIndex;

        final Configuration contextConf = Configurations.merge(
            ContextConfiguration.CONF
                .set(ContextConfiguration.IDENTIFIER,
                    contextId)
                .build(),
            psDriver.getWorkerContextConfiguration()
        );

        final Configuration serviceConf = psDriver.getWorkerServiceConfiguration(contextId);

        final Configuration taskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, UPDATER_TASK_PREFIX + workerIndex)
            .set(TaskConfiguration.TASK, UpdaterTask.class)
            .build();

        final Configuration parametersConf = Tang.Factory.getTang().newConfigurationBuilder()
            .bindNamedParameter(NumUpdates.class, Integer.toString(numUpdatesPerWorker))
            .bindNamedParameter(StartKey.class, Integer.toString(startKey))
            .bindNamedParameter(NumKeys.class, Integer.toString(numKeys))
            .build();

        allocatedEvaluator.submitContextAndServiceAndTask(contextConf, serviceConf,
            Configurations.merge(taskConf, parametersConf));
      }
    };
  }

  /**
   * Delegate AllocatedEvaluator event to evaluatorManager.
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      evaluatorManager.onEvaluatorAllocated(allocatedEvaluator);
    }
  }

  /**
   * Delegate ActiveContext event to evaluatorManager.
   */
  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext activeContext) {
      evaluatorManager.onContextActive(activeContext);
    }
  }

  /**
   * Log task running status.
   */
  final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask runningTask) {
      LOG.log(Level.INFO, "Task running: {0}", runningTask.getId());
    }
  }

  /**
   * When all UpdaterTasks are complete, run a ValidatorTask.
   * When the ValidatorTask is complete, shutdown the Parameter Service.
   * The Driver will then shutdown because it is idle.
   */
  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask completedTask) {
      LOG.log(Level.INFO, "Task completed: {0}", completedTask.getId());

      final int taskCompletedCount = taskCompletedCounter.incrementAndGet();
      if (taskCompletedCount < numWorkers) { // Close Contexts when their task is completed, up to the last Worker.
        workerContextsToClose.add(completedTask.getActiveContext());
        // completedTask.getActiveContext().close();

      } else if (taskCompletedCount == numWorkers) { // For the last Worker, run the Validator Task.
        LOG.log(Level.INFO, "All Tasks have completed, running Validator Task.");

        final Configuration taskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, VALIDATOR_TASK_ID)
            .set(TaskConfiguration.TASK, ValidatorTask.class)
            .build();

        final Configuration parameterConf = Tang.Factory.getTang().newConfigurationBuilder()
            .bindNamedParameter(NumWorkers.class, Integer.toString(numWorkers))
            .bindNamedParameter(NumUpdates.class, Integer.toString(numUpdatesPerWorker))
            .bindNamedParameter(StartKey.class, Integer.toString(startKey))
            .bindNamedParameter(NumKeys.class, Integer.toString(numKeys))
            .build();

        completedTask.getActiveContext().submitTask(Configurations.merge(taskConf, parameterConf));

      } else if (taskCompletedCount == numWorkers + 1) { // When the Validator Task is complete, close all contexts.
        workerContextsToClose.add(completedTask.getActiveContext());
        // completedTask.getActiveContext().close();

        LOG.log(Level.INFO, "Correct result validated, shutting down parameter server.");
        for (final ActiveContext context : workerContextsToClose) {
          context.close();
        }
        for (final ActiveContext context : psContexts) {
          context.close();
        }
      }
    }
  }
}
