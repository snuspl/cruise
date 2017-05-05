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
package edu.snu.cay.services.et.plan.impl;

import edu.snu.cay.services.et.common.util.concurrent.CompletedFuture;
import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.exceptions.PlanAlreadyExecutingException;
import edu.snu.cay.services.et.plan.api.Op;
import edu.snu.cay.services.et.plan.api.PlanExecutor;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A mock plan executor that executes plans by logging the planned actions.
 * The operators are assumed to be executed in BFS order, being included in the same log message
 * if the operators are in the same level in the DAG.
 */
public final class LoggingPlanExecutor implements PlanExecutor {
  private static final Logger LOG = Logger.getLogger(LoggingPlanExecutor.class.getName());

  @Inject
  private LoggingPlanExecutor() {
  }

  @Override
  public ListenableFuture<Void> execute(final ETPlan plan) throws PlanAlreadyExecutingException {
    final int numTotalOps = plan.getNumTotalOps();
    LOG.log(Level.INFO, "Num total ops: {0}", numTotalOps);

    final Set<Op> opsToExecute = plan.getInitialOps();
    while (!opsToExecute.isEmpty()) {
      LOG.log(Level.INFO, "Execute {0} ops: {1}", new Object[] {opsToExecute.size(), opsToExecute});
      final Set<Op> nextOps = new HashSet<>();
      opsToExecute.forEach(op -> nextOps.addAll(plan.onComplete(op)));
      opsToExecute.clear();
      opsToExecute.addAll(nextOps);
    }

    return new CompletedFuture<>(null);
  }

  @Override
  public void registerListener(final String id, final EventHandler<OpResult> callback) {
    // do nothing
  }

  @Override
  public void unregisterListener(final String id) {
    // do nothing
  }
}
