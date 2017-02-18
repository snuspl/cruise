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
package edu.snu.cay.services.et.driver.impl;

import edu.snu.cay.services.et.common.impl.CallbackRegistry;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.util.concurrent.*;

/**
 * Implementation for {@link AllocatedExecutor}.
 */
@DriverSide
final class AllocatedExecutorImpl implements AllocatedExecutor {
  private final ActiveContext etContext;
  private final String identifier;
  private final CallbackRegistry callbackRegistry;

  AllocatedExecutorImpl(final ActiveContext etContext,
                        final CallbackRegistry callbackRegistry) {
    this.etContext = etContext;
    this.identifier = etContext.getEvaluatorId();
    this.callbackRegistry = callbackRegistry;
  }

  @Override
  public String getId() {
    return identifier;
  }

  @Override
  public Future<TaskResult> submitTask(final Configuration taskConf) {
    try {
      final String taskId = Tang.Factory.getTang().newInjector(taskConf)
          .getNamedInstance(TaskConfigurationOptions.Identifier.class);

      final TaskResultFuture resultFuture = new TaskResultFuture(taskId);
      callbackRegistry.register(TaskResult.class, taskId, resultFuture::setResult);

      etContext.submitTask(taskConf);

      return resultFuture;

    } catch (final InjectionException e) {
      throw new RuntimeException("Task id should exist within task configuration", e);
    }
  }

  @Override
  public void close() {

    // simply close the et context, which is a root context of evaluator.
    // so evaluator will be released
    etContext.close();
  }

  private final class TaskResultFuture implements Future<TaskResult> {
    private final String taskId;
    private final CountDownLatch completedLatch = new CountDownLatch(1);
    private volatile TaskResult taskResult;

    TaskResultFuture(final String taskId) {
      this.taskId = taskId;
    }

    /**
     * Sets the result of task.
     * It will wakes up threads waiting in {@link #get()} and {@link #get(long, TimeUnit)}.
     * @param result a result of finished task
     */
    void setResult(final TaskResult result) {
      taskResult = result;
      completedLatch.countDown();
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
      // do not allow cancel
      return false;
    }

    @Override
    public boolean isCancelled() {
      // do not allow cancel
      return false;
    }

    @Override
    public boolean isDone() {
      return completedLatch.getCount() == 0;
    }

    @Override
    public TaskResult get() throws InterruptedException, ExecutionException {
      completedLatch.await();
      return taskResult;
    }

    @Override
    public TaskResult get(final long timeout, final TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if (!completedLatch.await(timeout, unit)) {
        throw  new TimeoutException(String.format("Timeout while waiting for the result of task %s", taskId));
      }
      return taskResult;
    }
  }
}
