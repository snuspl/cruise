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

import edu.snu.cay.services.ps.driver.ParameterServerDriver;
import edu.snu.cay.services.ps.examples.parameters.NumKeys;
import edu.snu.cay.services.ps.examples.parameters.NumUpdates;
import edu.snu.cay.services.ps.examples.parameters.NumWorkers;
import edu.snu.cay.services.ps.examples.parameters.StartKey;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
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

  private static final String PS_ID = "PS";
  private static final String WORKER_CONTEXT_PREFIX = "Worker-Context-";
  private static final String UPDATER_TASK_PREFIX = "Updater-Task-";
  private static final String VALIDATOR_TASK_ID = "Validator-Task";

  private final ParameterServerDriver psDriver;
  private final EvaluatorRequestor evalRequestor;

  private final int numWorkers;
  private final int numUpdatesPerWorker;
  private final int startKey;
  private final int numKeys;

  private final AtomicInteger evalCounter = new AtomicInteger(0);
  private final AtomicInteger taskCompletedCounter = new AtomicInteger(0);

  private ActiveContext psActiveContext = null;

  /**
   * numUpdates is the _total_ number of updates to push.
   * This value is then divided by numWorkers and passed as numUpdatesPerWorker to the Tasks.
   *
   * @throws IllegalArgumentException numUpdates must be divisible by numWorkers and (numWorkers * numKeys).
   *                                  If not, the exception is thrown.
   *                                  This makes accounting of updates across Workers much simpler.
   */
  @Inject
  private PSExampleDriver(final ParameterServerDriver psDriver,
                          final EvaluatorRequestor evalRequestor,
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
    this.evalRequestor = evalRequestor;
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
      evalRequestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(numWorkers + 1)
          .setMemory(512)
          .setNumberOfCores(1)
          .build());
    }
  }

  /**
   * Submit PS Context and Services, and run UpdaterTasks.
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final int evalCount = evalCounter.incrementAndGet();

      if (evalCount == 1) { // Submit the ParameterServer

        final Configuration contextConf = Configurations.merge(
            ContextConfiguration.CONF
                .set(ContextConfiguration.IDENTIFIER, PS_ID)
                .build(),
            psDriver.getContextConfiguration());

        final Configuration serviceConf = psDriver.getServerServiceConfiguration();

        allocatedEvaluator.submitContextAndService(contextConf, serviceConf);

      } else { // Submit a Context and Service for the PS Worker, and run an UpdaterTask on top of that

        final Configuration contextConf = Configurations.merge(
            ContextConfiguration.CONF
                .set(ContextConfiguration.IDENTIFIER,
                    WORKER_CONTEXT_PREFIX + (evalCount - 1))
                .build(),
            psDriver.getContextConfiguration()
        );

        final Configuration serviceConf = psDriver.getWorkerServiceConfiguration();

        final Configuration taskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, UPDATER_TASK_PREFIX + (evalCount - 1))
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
    }
  }

  /**
   * Save the Parameter Server's active context.
   */
  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext activeContext) {
      LOG.log(Level.INFO, "Context active: {0}", activeContext.getId());
      if (activeContext.getId().equals(PS_ID)) {
        psActiveContext = activeContext;
      }
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
        completedTask.getActiveContext().close();

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

      } else if (taskCompletedCount == numWorkers + 1) { // When the Validator Task is complete, shutdown the PS Server.
        completedTask.getActiveContext().close();

        LOG.log(Level.INFO, "Correct result validated, shutting down parameter server.");
        psActiveContext.close();
      }
    }
  }
}
