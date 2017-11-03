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

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 * A handler of CompletedEvaluator event.
 * It passes the event to the {@link ExecutorManager} to remove the entry.
 */
@Private
@DriverSide
public final class EvaluatorCompletedHandler implements EventHandler<CompletedEvaluator> {
  private final ExecutorManager executorManager;

  @Inject
  private EvaluatorCompletedHandler(final ExecutorManager executorManager) {
    this.executorManager = executorManager;
  }

  @Override
  public void onNext(final CompletedEvaluator completedEvaluator) {
    executorManager.onClosedExecutor(completedEvaluator.getId());
  }
}
