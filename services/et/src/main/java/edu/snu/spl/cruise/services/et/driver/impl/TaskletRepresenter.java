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
package edu.snu.spl.cruise.services.et.driver.impl;

import edu.snu.spl.cruise.services.et.common.impl.CallbackRegistry;
import edu.snu.spl.cruise.services.et.common.util.concurrent.ResultFuture;
import edu.snu.spl.cruise.services.et.driver.api.MessageSender;
import edu.snu.spl.cruise.utils.StateMachine;

import java.util.Optional;

/**
 * Represents a Tasklet.
 * It manages tasklet status tracking tasklet events.
 */
public final class TaskletRepresenter {
  private final String executorId;
  private final String taskletId;

  private final MessageSender msgSender;

  private final CallbackRegistry callbackRegistry;

  private Optional<RunningTasklet> runningTaskletOptional = Optional.empty();

  private final StateMachine stateMachine;

  public enum State {
    INIT,
    RUNNING,
    DONE,
    FAILED
  }

  TaskletRepresenter(final String executorId,
                     final String taskletId,
                     final MessageSender msgSender,
                     final CallbackRegistry callbackRegistry) {
    this.executorId = executorId;
    this.taskletId = taskletId;
    this.msgSender = msgSender;
    this.callbackRegistry = callbackRegistry;
    this.stateMachine = initStateMachine();
  }

  private StateMachine initStateMachine() {
    return StateMachine.newBuilder()
        .addState(State.INIT, "Tasklet has been submitted to executor, but there's no response message yet.")
        .addState(State.RUNNING, "Tasklet is running on executor.")
        .addState(State.DONE, "Tasklet has been finished successfully. It may be closed by Tasklet::close().")
        .addState(State.FAILED, "Tasklet has been failed during execution.")
        .addTransition(State.INIT, State.RUNNING, "Received tasklet running status message after submission.")
        .addTransition(State.RUNNING, State.DONE, "Received tasklet done status message.")
        .addTransition(State.RUNNING, State.FAILED, "Received tasklet failed status message.")
        .setInitialState(State.INIT)
        .build();
  }

  /**
   * @return True if this tasklet has been finished
   */
  public boolean isFinished() {
    final State state = (State) stateMachine.getCurrentState();
    return state.equals(State.DONE) || state.equals(State.FAILED);
  }

  /**
   * @return an {@link Optional} with {@link RunningTasklet} or empty when it does not receive Running status message
   */
  public Optional<RunningTasklet> getRunningTasklet() {
    return runningTaskletOptional;
  }

  /**
   * On Running status message from executor.
   */
  public synchronized void onRunningStatusMsg() {
    if (stateMachine.getCurrentState() == State.INIT) {
      stateMachine.setState(State.RUNNING);
      final ResultFuture<TaskletResult> taskletResultFuture = new ResultFuture<>();
      callbackRegistry.register(TaskletResult.class, executorId + taskletId, taskletResultFuture::onCompleted);

      final RunningTasklet runningTasklet = new RunningTasklet(executorId, taskletId,
          this, taskletResultFuture, msgSender);
      runningTaskletOptional = Optional.of(runningTasklet);
      callbackRegistry.onCompleted(RunningTasklet.class, executorId + taskletId, runningTasklet);
    }
  }

  private synchronized void onDoneFailedStatusMsg(final State state) {
    if (stateMachine.getCurrentState() == State.INIT) { // task finished before running msg
      onRunningStatusMsg();
    }

    if (stateMachine.getCurrentState() == State.RUNNING) {
      stateMachine.setState(state);
      final TaskletResult taskletResult = new TaskletResult(taskletId, state);
      callbackRegistry.onCompleted(TaskletResult.class, executorId + taskletId, taskletResult);
    } else {
      throw new IllegalStateException();
    }
  }

  /**
   * On Done status message from executor.
   */
  public synchronized void onDoneStatusMsg() {
    onDoneFailedStatusMsg(State.DONE);
  }

  /**
   * On Failed status message from executor.
   */
  public synchronized void onFailedStatusMsg() {
    onDoneFailedStatusMsg(State.FAILED);
  }

  /**
   * @return a tasklet identifier
   */
  public String getId() {
    return taskletId;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TaskletRepresenter)) {
      return false;
    }

    final TaskletRepresenter that = (TaskletRepresenter) o;

    return executorId.equals(that.executorId) && taskletId.equals(that.taskletId);
  }

  @Override
  public int hashCode() {
    int result = executorId.hashCode();
    result = 31 * result + taskletId.hashCode();
    return result;
  }
}
