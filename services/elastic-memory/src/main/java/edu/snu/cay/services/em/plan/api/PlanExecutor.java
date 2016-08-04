/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.services.em.plan.api;

import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.RunningTask;

import java.util.concurrent.Future;

/**
 * A plan executor interface.
 */
public interface PlanExecutor {

  /**
   * Execute a plan with new thread and return a future.
   * Note that in default it does not support concurrent execution of multiple plans.
   *
   * @param plan to execute
   * @return a Future that summarizes a plan execution when it has finished
   */
  Future<PlanResult> execute(Plan plan);

  /**
   * Receive a RunningTask event from the Runtime.
   * Each PlanExecutor should implement this handler to keep track of newly submitted tasks.
   * @param task the running task
   */
  void onRunningTask(RunningTask task);

  /**
   * Receive a CompletedTask event from the Runtime.
   * @param task the completed task
   */
  void onCompletedTask(CompletedTask task);
}
