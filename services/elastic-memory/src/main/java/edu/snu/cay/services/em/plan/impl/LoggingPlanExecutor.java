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
package edu.snu.cay.services.em.plan.impl;

import edu.snu.cay.services.em.plan.api.TransferStep;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.PlanExecutor;
import edu.snu.cay.services.em.plan.api.PlanResult;
import org.apache.reef.driver.task.RunningTask;

import javax.inject.Inject;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A mock plan executor that executes plans by logging the planned actions within a new thread.
 * A real single-threaded executor implementation could follow a similar pattern.
 */
public final class LoggingPlanExecutor implements PlanExecutor {
  private static final String NAMESPACE = "DOLPHIN_BSP";
  private static final Logger LOG = Logger.getLogger(LoggingPlanExecutor.class.getName());

  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  @Inject
  private LoggingPlanExecutor() {
  }

  @Override
  public Future<PlanResult> execute(final Plan plan) {
    return executor.submit(new Callable<PlanResult>() {
      @Override
      public PlanResult call() throws Exception {
        for (final String evaluatorToAdd : plan.getEvaluatorsToAdd(NAMESPACE)) {
          LOG.log(Level.INFO, "Add evaluator: " + evaluatorToAdd);
        }

        for (final TransferStep transferStep : plan.getTransferSteps(NAMESPACE)) {
          LOG.log(Level.INFO, "Apply transfer step: " + transferStep);
        }

        for (final String evaluatorToDelete : plan.getEvaluatorsToDelete(NAMESPACE)) {
          LOG.log(Level.INFO, "Delete evaluator: " + evaluatorToDelete);
        }

        LOG.log(Level.INFO, "Done.");

        return new PlanResultImpl();
      }
    });
  }

  @Override
  public void onRunningTask(final RunningTask task) {
    LOG.log(Level.INFO, "RunningTask {0}", task);
  }
}
