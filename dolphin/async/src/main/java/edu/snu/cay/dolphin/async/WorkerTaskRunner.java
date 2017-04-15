/*
 * Copyright (C) 2017 Seoul National University
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

import edu.snu.cay.dolphin.async.optimizer.ETOptimizationOrchestrator;
import edu.snu.cay.dolphin.async.optimizer.parameters.OptimizationIntervalMs;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.SubmittedTask;
import edu.snu.cay.services.et.driver.impl.TaskResult;
import edu.snu.cay.services.et.exceptions.ExecutorNotExistException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class for running worker tasks.
 */
@DriverSide
public final class WorkerTaskRunner {
  private static final Logger LOG = Logger.getLogger(WorkerTaskRunner.class.getName());

  private final ETMaster etMaster;

  private final InjectionFuture<ETDolphinDriver> etDolphinDriverFuture;

  private final WorkerStateManager workerStateManager;

  private final InjectionFuture<ETOptimizationOrchestrator> optimizationOrchestrator;

  private final long optimizationIntervalMs;

  private final Map<String, SubmittedTask> executorIdToTask = new ConcurrentHashMap<>();

  @Inject
  private WorkerTaskRunner(final InjectionFuture<ETOptimizationOrchestrator> optimizationOrchestrator,
                           final InjectionFuture<ETDolphinDriver> etDolphinDriverFuture,
                           final ETMaster etMaster,
                           final WorkerStateManager workerStateManager,
                           @Parameter(OptimizationIntervalMs.class) final long optimizationIntervalMs,
                           @Parameter(DolphinParameters.NumWorkers.class) final int numWorkers) {
    this.optimizationOrchestrator = optimizationOrchestrator;
    this.etMaster = etMaster;
    this.etDolphinDriverFuture = etDolphinDriverFuture;
    this.workerStateManager = workerStateManager;
    this.optimizationIntervalMs = optimizationIntervalMs;
    LOG.log(Level.INFO, "Initialized with NumWorkers: {0}", numWorkers);
  }

  /**
   * Runs tasks on worker executors with optimization.
   * With optimization the number of workers varies during runtime.
   * @param workers a set of initial worker executors
   * @return a list of {@link TaskResult}
   */
  public List<TaskResult> runWithOptimization(final List<AllocatedExecutor> workers) {
    final Map<String, Future<SubmittedTask>> executorIdToTaskFuture = new HashMap<>(workers.size());
    workers.forEach(worker -> executorIdToTaskFuture.put(worker.getId(),
        worker.submitTask(etDolphinDriverFuture.get().getWorkerTaskConf())));

    executorIdToTaskFuture.forEach((executorId, taskFuture) -> {
      try {
        executorIdToTask.put(executorId, taskFuture.get());
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException("Exception while waiting for tasks to be submitted", e);
      }
    });

    LOG.log(Level.INFO, "Waiting workers to finish INIT stage");
    // wait until entering run state
    workerStateManager.waitWorkersToFinishInitStage();

    LOG.log(Level.INFO, "Start trying optimization");
    // keep running optimization while its available to optimization
    while (workerStateManager.tryEnterOptimization()) {
      final Pair<Set<String>, Set<String>> changesInWorkers = optimizationOrchestrator.get().optimize();
      updateTaskEntry(changesInWorkers.getLeft(), changesInWorkers.getRight());
      workerStateManager.onOptimizationFinished(changesInWorkers.getLeft(), changesInWorkers.getRight());

      try {
        LOG.log(Level.INFO, "Sleep {0} ms for next optimization", optimizationIntervalMs);
        Thread.sleep(optimizationIntervalMs);
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "Interrupted while sleeping for next optimization try." +
            " Let's try optimization now.", e);
      }
    }

    LOG.log(Level.INFO, "Wait and get task results");
    // waiting for to complete
    return waitAndGetTaskResult();
  }

  private void updateTaskEntry(final Set<String> addedWorkers,
                               final Set<String> deletedWorkers) {
    for (final String addedExecutorId : addedWorkers) {
      final SubmittedTask task;
      try {
        final Optional<SubmittedTask> taskOptional = etMaster.getExecutor(addedExecutorId).getRunningTask();
        if (!taskOptional.isPresent()) {
          throw new RuntimeException("Task should be running on the executor");
        }
        task = taskOptional.get();
      } catch (ExecutorNotExistException e) {
        throw new RuntimeException(e);
      }
      executorIdToTask.put(addedExecutorId, task);
    }
    executorIdToTask.keySet().removeAll(deletedWorkers);
  }

  private List<TaskResult> waitAndGetTaskResult() {
    final List<TaskResult> taskResultList = new ArrayList<>(executorIdToTask.size());

    executorIdToTask.values().forEach(task -> {
      try {
        taskResultList.add(task.getTaskResult());
      } catch (InterruptedException e) {
        throw new RuntimeException("Exception while waiting for the task results", e);
      }
    });

    LOG.log(Level.INFO, "Task finished");
    return taskResultList;
  }
}
