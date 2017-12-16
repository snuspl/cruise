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

import edu.snu.spl.cruise.services.et.avro.TaskletStatusMsg;
import edu.snu.spl.cruise.services.et.common.impl.CallbackRegistry;
import edu.snu.spl.cruise.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.spl.cruise.services.et.common.util.concurrent.ResultFuture;
import edu.snu.spl.cruise.services.et.configuration.TaskletConfiguration;
import edu.snu.spl.cruise.services.et.driver.api.AllocatedExecutor;
import edu.snu.spl.cruise.services.et.driver.api.MessageSender;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation for {@link AllocatedExecutor}.
 * It maintains a pointer for REEF's context, which is a core of executor runtime, and
 * {@link TaskletRepresenter}s running on the executor.
 */
@DriverSide
final class AllocatedExecutorImpl implements AllocatedExecutor {
  private final ActiveContext etContext;
  private final String identifier;
  private final CallbackRegistry callbackRegistry;
  private final ResultFuture<Void> closedFuture;
  private final MessageSender msgSender;

  private final Map<String, TaskletRepresenter> taskletRepresenterMap = new ConcurrentHashMap<>();

  AllocatedExecutorImpl(final ActiveContext etContext,
                        final MessageSender msgSender,
                        final CallbackRegistry callbackRegistry) {
    this.etContext = etContext;
    this.identifier = etContext.getEvaluatorId();
    this.msgSender = msgSender;
    this.callbackRegistry = callbackRegistry;
    this.closedFuture = new ResultFuture<>();
  }

  @Override
  public String getId() {
    return identifier;
  }

  @Override
  public ListenableFuture<RunningTasklet> submitTasklet(final TaskletConfiguration taskletConf) {
    final String taskletId = taskletConf.getId();

    final ResultFuture<RunningTasklet> runningTaskletFuture = new ResultFuture<>();
    callbackRegistry.register(RunningTasklet.class, identifier + taskletId, runningTaskletFuture::onCompleted);
    taskletRepresenterMap.put(taskletId, new TaskletRepresenter(identifier, taskletId, msgSender, callbackRegistry));

    msgSender.sendTaskletStartMsg(identifier, taskletId, taskletConf.getConfiguration());

    return runningTaskletFuture;
  }

  @Override
  public void onTaskletStatusMessage(final String taskletId, final TaskletStatusMsg taskletStatusMsg) {
    final TaskletRepresenter taskletRepresenter = taskletRepresenterMap.get(taskletId);
    switch (taskletStatusMsg.getType()) {
    case Running:
      taskletRepresenter.onRunningStatusMsg();
      break;
    case Done:
      taskletRepresenter.onDoneStatusMsg();
      taskletRepresenterMap.remove(taskletId);
      break;
    case Failed:
      taskletRepresenter.onFailedStatusMsg();
      taskletRepresenterMap.remove(taskletId);
      break;
    default:
      throw new RuntimeException("unexpected msg type");
    }
  }

  @Override
  public Set<String> getTaskletIds() {
    return taskletRepresenterMap.keySet();
  }

  @Override
  public RunningTasklet getRunningTasklet(final String taskletId) {
    return taskletRepresenterMap.get(taskletId).getRunningTasklet().get();
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
