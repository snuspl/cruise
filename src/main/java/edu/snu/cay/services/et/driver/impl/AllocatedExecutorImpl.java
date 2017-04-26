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
package edu.snu.cay.services.et.driver.impl;

import edu.snu.cay.services.et.common.impl.CallbackRegistry;
import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.common.util.concurrent.ResultFuture;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.util.Optional;

/**
 * Implementation for {@link AllocatedExecutor}.
 */
@DriverSide
final class AllocatedExecutorImpl implements AllocatedExecutor {
  private final ActiveContext etContext;
  private final String identifier;
  private final CallbackRegistry callbackRegistry;
  private final ResultFuture<Void> closedFuture;
  private volatile SubmittedTask runningTask;

  AllocatedExecutorImpl(final ActiveContext etContext,
                        final CallbackRegistry callbackRegistry) {
    this.etContext = etContext;
    this.identifier = etContext.getEvaluatorId();
    this.callbackRegistry = callbackRegistry;
    this.closedFuture = new ResultFuture<>();
  }

  @Override
  public String getId() {
    return identifier;
  }

  @Override
  public ListenableFuture<SubmittedTask> submitTask(final Configuration taskConf) {
    try {
      final String taskId = Tang.Factory.getTang().newInjector(taskConf)
          .getNamedInstance(TaskConfigurationOptions.Identifier.class);

      final ResultFuture<SubmittedTask> submittedTaskFuture = new ResultFuture<>();
      callbackRegistry.register(SubmittedTask.class, taskId, submittedTaskFuture::onCompleted);

      final ResultFuture<TaskResult> resultFuture = new ResultFuture<>();
      resultFuture.addListener(taskResult -> runningTask = null);
      callbackRegistry.register(TaskResult.class, taskId, resultFuture::onCompleted);

      final ResultFuture<RunningTask> runningTaskFuture = new ResultFuture<>();
      runningTaskFuture.addListener(task -> {
        final SubmittedTask submittedTask = new SubmittedTask(task, resultFuture);
        runningTask = submittedTask;
        callbackRegistry.onCompleted(SubmittedTask.class, taskId, submittedTask);
      });
      callbackRegistry.register(RunningTask.class, taskId, runningTaskFuture::onCompleted);

      etContext.submitTask(taskConf);

      return submittedTaskFuture;
    } catch (final InjectionException e) {
      throw new RuntimeException("Task id should exist within task configuration", e);
    }
  }

  @Override
  public Optional<SubmittedTask> getRunningTask() {
    return Optional.ofNullable(runningTask);
  }

  /**
   * Completes future returned by {@link #close()}.
   * It should be called upon the completion of closing executor.
   */
  void onFinishClose() {
    closedFuture.onCompleted(null);
  }

  @Override
  public ListenableFuture<Void> close() {

    // simply close the et context, which is a root context of evaluator.
    // so evaluator will be released
    etContext.close();

    return closedFuture;
  }
}
