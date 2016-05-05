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
package edu.snu.cay.async.optimizer;

import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.PlanExecutor;
import edu.snu.cay.services.em.plan.api.PlanResult;
import edu.snu.cay.services.em.plan.api.TransferStep;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of Plan Executor for AsyncDolphin.
 */
public final class AsyncDolphinPlanExecutor implements PlanExecutor {
  private static final Logger LOG = Logger.getLogger(AsyncDolphinPlanExecutor.class.getName());
  private static final String EM_CONTEXT_ID_PREFIX = "EM-Context-";
  private static final int NUM_ADD = 3;
  private final ElasticMemory serverEM;
  private final ElasticMemory workerEM;
  private final CountDownLatch allocationCounter = new CountDownLatch(NUM_ADD);
  private final CountDownLatch callbackCounter = new CountDownLatch(NUM_ADD);

  @Inject
  private AsyncDolphinPlanExecutor(@Parameter(ServerEM.class) final ElasticMemory serverEM,
                                   @Parameter(WorkerEM.class) final ElasticMemory workerEM) {
    this.serverEM = serverEM;
    this.workerEM = workerEM;
  }

  @Override
  public Future<PlanResult> execute(final Plan plan) {
    final Collection<String> evalsToAdd = plan.getEvaluatorsToAdd();
    final Collection<String> evalsToDel = plan.getEvaluatorsToDelete();
    final Collection<TransferStep> transferSteps = plan.getTransferSteps();

    final EventHandler<AllocatedEvaluator> wEvaluatorAllocatedHandler = new EventHandler<AllocatedEvaluator>() {
      @Override
      public void onNext(final AllocatedEvaluator allocatedEvaluator) {
        LOG.log(Level.INFO, "Worker EM add allocated evaluator {0} successfully.", allocatedEvaluator);
        final Configuration dummyContextConf = ContextConfiguration.CONF.set(ContextConfiguration.IDENTIFIER,
            EM_CONTEXT_ID_PREFIX + (NUM_ADD - allocationCounter.getCount())).build();
        allocationCounter.countDown();
        allocatedEvaluator.submitContext(dummyContextConf);
      }
    };
    final EventHandler<AllocatedEvaluator> sEvaluatorAllocatedHandler = new EventHandler<AllocatedEvaluator>() {
      @Override
      public void onNext(final AllocatedEvaluator allocatedEvaluator) {
        LOG.log(Level.INFO, "Server EM add allocated evaluator {0} successfully.", allocatedEvaluator);
        final Configuration dummyContextConf = ContextConfiguration.CONF.set(ContextConfiguration.IDENTIFIER,
            EM_CONTEXT_ID_PREFIX + (NUM_ADD - allocationCounter.getCount())).build();
        allocationCounter.countDown();
        allocatedEvaluator.submitContext(dummyContextConf);
      }
    };
    final List<EventHandler<ActiveContext>> sContextActiveHandlers = new ArrayList<>();
    sContextActiveHandlers.add(new EventHandler<ActiveContext>() {
      // Checks that EM add callback was actually triggered
      @Override
      public void onNext(final ActiveContext activeContext) {
        LOG.log(Level.INFO, "Server EM add callback for active context {0} triggered successfully.", activeContext);
        callbackCounter.countDown();
      }
    });
    final List<EventHandler<ActiveContext>> wContextActiveHandlers = new ArrayList<>();
    wContextActiveHandlers.add(new EventHandler<ActiveContext>() {
      // Checks that EM add callback was actually triggered
      @Override
      public void onNext(final ActiveContext activeContext) {
        LOG.log(Level.INFO, "Worker EM add callback for active context {0} triggered successfully.", activeContext);
        callbackCounter.countDown();
      }
    });
    serverEM.add(3, 1024, 1, sEvaluatorAllocatedHandler, sContextActiveHandlers);
    workerEM.add(4, 1024, 1, wEvaluatorAllocatedHandler, wContextActiveHandlers);
    return null;
  }

  @Override
  public void onRunningTask(final RunningTask task) {

  }
}
