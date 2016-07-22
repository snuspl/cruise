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
package edu.snu.cay.services.em.examples.group;

import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.evalmanager.api.EvaluatorManager;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
// import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver code for GroupEM.
 */
@Unit
public final class GroupEMDriver {
  private final ElasticMemory elasticMemory;
  private final EvaluatorRequestor requestor;
  private final AtomicInteger contextIndex;
  private final EvaluatorManager evaluatorManager;

  private static final Logger LOG = Logger.getLogger(GroupEMDriver.class.getName());

  private static final String EM_CONTEXT_ID_PREFIX = "GroupEM-";

  @Inject
  private GroupEMDriver(final ElasticMemory elasticMemory,
                        final EvaluatorRequestor requestor,
                        final EvaluatorManager evaluatorManager) {
    this.elasticMemory = elasticMemory;
    this.requestor = requestor;
    this.evaluatorManager = evaluatorManager;
    elasticMemory.addGroup("default");
    contextIndex = new AtomicInteger(0);
  }

  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      final EventHandler<AllocatedEvaluator> evaluatorAllocatedHandler = new EventHandler<AllocatedEvaluator>() {
        @Override
        public void onNext(final AllocatedEvaluator allocatedEvaluator) {
          LOG.log(Level.INFO, "EM add allocated evaluator {0} successfully.", allocatedEvaluator);
          final Configuration dummyContextConf = ContextConfiguration.CONF.set(ContextConfiguration.IDENTIFIER,
              EM_CONTEXT_ID_PREFIX + (contextIndex.getAndIncrement())).build();
          allocatedEvaluator.submitContext(dummyContextConf);
        }
      };
      final List<EventHandler<ActiveContext>> contextActiveHandlers = new ArrayList<>();
      contextActiveHandlers.add(new EventHandler<ActiveContext>() {
        @Override
        public void onNext(final ActiveContext activeContext) {
          LOG.log(Level.INFO, "EM add callback for active context {0} triggered successfully.", activeContext);
          activeContext.close();
        }
      });
      elasticMemory.add("default", 10, 1024, 1, evaluatorAllocatedHandler, contextActiveHandlers);
    }
  }

  public final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      evaluatorManager.onEvaluatorAllocated(allocatedEvaluator);
    }
  }

  public final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext activeContext) {
      evaluatorManager.onContextActive(activeContext);
    }
  }
}
