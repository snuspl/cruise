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

import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.SubmittedTask;
import edu.snu.cay.services.et.driver.impl.TaskResult;
import edu.snu.cay.services.et.exceptions.ExecutorNotExistException;
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
 * A class for running tasks in server and worker executors.
 * It responds to the change of the entry of worker/server executors.
 * It also tracks active worker tasks for {@link #waitAndGetTaskResult()}.
 */
@DriverSide
public final class ETTaskRunner {
  private static final Logger LOG = Logger.getLogger(ETTaskRunner.class.getName());

  private final ETMaster etMaster;

  private final InjectionFuture<ETDolphinDriver> etDolphinDriverFuture;

  private final WorkerStateManager workerStateManager;

  private final Map<String, AllocatedExecutor> workerExecutors = new ConcurrentHashMap<>();
  private final Map<String, AllocatedExecutor> serverExecutors = new ConcurrentHashMap<>();

  private final Map<String, SubmittedTask> executorIdToTask = new ConcurrentHashMap<>();

  @Inject
  private ETTaskRunner(final InjectionFuture<ETDolphinDriver> etDolphinDriverFuture,
                       final ETMaster etMaster,
                       final WorkerStateManager workerStateManager,
                       @Parameter(DolphinParameters.NumWorkers.class) final int numWorkers) {
    this.etMaster = etMaster;
    this.etDolphinDriverFuture = etDolphinDriverFuture;
    this.workerStateManager = workerStateManager;
    LOG.log(Level.INFO, "Initialized with NumWorkers: {0}", numWorkers);
  }

  /**
   * Runs tasks on worker executors. It returns when all the worker task finish.
   * With optimization, the number of workers changes during runtime by {@link #updateExecutorEntry}.
   * @param workers a set of initial worker executors
   * @return a list of {@link TaskResult}
   */
  public List<TaskResult> run(final List<AllocatedExecutor> workers,
                              final List<AllocatedExecutor> servers) {
    workers.forEach(worker -> workerExecutors.put(worker.getId(), worker));
    servers.forEach(server -> serverExecutors.put(server.getId(), server));

    // submit dummy tasks to servers
    servers.forEach(server -> server.submitTask(etDolphinDriverFuture.get().getServerTaskConf()));

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

    LOG.log(Level.INFO, "Wait for workers to finish run stage");

    workerStateManager.waitWorkersToFinishRunStage();

    workers.clear();
    workers.addAll(workerExecutors.values());

    servers.clear();
    servers.addAll(serverExecutors.values());

    LOG.log(Level.INFO, "Wait and get task results");
    // waiting for to complete
    return waitAndGetTaskResult();
  }

  /**
   * Updates the entry of worker tasks, which is called by the Optimization orchestrator.
   * @param addedWorkers a set of added worker tasks
   * @param deletedWorkers a set of deleted worker tasks
   */
  public void updateExecutorEntry(final Set<String> addedWorkers,
                                  final Set<String> deletedWorkers,
                                  final Set<String> addedServers,
                                  final Set<String> deletedServers) {
    for (final String addedWorker : addedWorkers) {
      final SubmittedTask task;
      final AllocatedExecutor executor;
      try {
        executor = etMaster.getExecutor(addedWorker);
        final Optional<SubmittedTask> taskOptional = executor.getRunningTask();
        if (!taskOptional.isPresent()) {
          throw new RuntimeException(String.format("Task is not running on the executor %s", addedWorker));
        }
        task = taskOptional.get();
      } catch (ExecutorNotExistException e) {
        throw new RuntimeException(e);
      }

      workerExecutors.put(executor.getId(), executor);
      executorIdToTask.put(addedWorker, task);
    }

    for (final String addedServer : addedServers) {
      final AllocatedExecutor executor;
      try {
        executor = etMaster.getExecutor(addedServer);
      } catch (ExecutorNotExistException e) {
        throw new RuntimeException(e);
      }
      serverExecutors.put(executor.getId(), executor);
    }

    workerExecutors.keySet().removeAll(deletedWorkers);
    serverExecutors.keySet().removeAll(deletedServers);
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
