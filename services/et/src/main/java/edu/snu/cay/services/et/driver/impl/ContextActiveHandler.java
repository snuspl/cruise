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

import edu.snu.cay.services.evalmanager.api.EvaluatorManager;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 * A handler of ActiveContext event.
 * It passes the event to the {@link EvaluatorManager} to progress the work.
 */
@Private
@DriverSide
public final class ContextActiveHandler implements EventHandler<ActiveContext> {
  private final EvaluatorManager evaluatorManager;

  @Inject
  private ContextActiveHandler(final EvaluatorManager evaluatorManager) {
    this.evaluatorManager = evaluatorManager;
  }

  /**
   * Passes the active context event to EvaluatorManager.
   * @param activeContext the context activated
   */
  @Override
  public void onNext(final ActiveContext activeContext) {
    evaluatorManager.onContextActive(activeContext);
  }
}
