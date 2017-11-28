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

import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.common.util.concurrent.ResultFuture;
import edu.snu.cay.services.et.driver.api.MessageSender;
import org.apache.reef.exception.evaluator.NetworkException;

import java.util.concurrent.ExecutionException;

/**
 * Represents a running Tasklet.
 */
public final class RunningTasklet {
  private final String executorId;
  private final String taskletId;
  private final TaskletRepresenter taskletRepresenter;
  private final ResultFuture<TaskletResult> taskletResultFuture;

  private final MessageSender msgSender;

  RunningTasklet(final String executorId,
                 final String taskletId,
                 final TaskletRepresenter taskletRepresenter,
                 final ResultFuture<TaskletResult> taskletResultFuture,
                 final MessageSender msgSender) {
    this.executorId = executorId;
    this.taskletId = taskletId;
    this.taskletRepresenter = taskletRepresenter;
    this.taskletResultFuture = taskletResultFuture;
    this.msgSender = msgSender;
  }

  public String getId() {
    return taskletId;
  }

  /**
   * Stops the running task.
   * @return the future of task result
   */
  public ListenableFuture<TaskletResult> stop() {
    if (!taskletRepresenter.isFinished()) {
      msgSender.sendTaskletStopMsg(executorId, taskletId);
    }
    return taskletResultFuture;
  }

  public void send(final byte[] message) throws NetworkException {
    if (!taskletRepresenter.isFinished()) {
      msgSender.sendTaskletCustomMsg(executorId, taskletId, message);
    }
  }

  /**
   * @return the result of this task, after waiting it to complete
   * @throws InterruptedException when interrupted while waiting
   */
  public TaskletResult getTaskResult() throws InterruptedException {
    try {
      return taskletResultFuture.get();
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RunningTasklet)) {
      return false;
    }

    final RunningTasklet that = (RunningTasklet) o;

    return taskletId.equals(that.taskletId);
  }

  @Override
  public int hashCode() {
    return taskletId.hashCode();
  }
}
