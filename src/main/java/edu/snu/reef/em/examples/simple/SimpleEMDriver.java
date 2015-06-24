/**
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

package edu.snu.reef.em.examples.simple;

import edu.snu.reef.em.driver.api.ElasticMemory;
import edu.snu.reef.em.driver.ElasticMemoryConfiguration;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.driver.task.TaskMessage;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * Driver code for EMExample
 */
@Unit
public final class SimpleEMDriver {
  private static final Logger LOG = Logger.getLogger(SimpleEMDriver.class.getName());
  private static final String CONTEXT_ID_PREFIX = "CmpContext-";
  private static final String TASK_ID_PREFIX = "CmpTask-";

  private final EvaluatorRequestor requestor;

  private final ElasticMemoryConfiguration emConf;
  private final ElasticMemory emService;

  @Inject
  public SimpleEMDriver(final EvaluatorRequestor requestor,
                        final ElasticMemoryConfiguration emConf,
                        final ElasticMemory emService) throws InjectionException {
    this.requestor = requestor;
    this.emConf = emConf;
    this.emService = emService;
  }

  /**
   * Spawn two small containers.
   */
  public final class DriverStartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      SimpleEMDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(2)
          .setMemory(64)
          .setNumberOfCores(1)
          .build());
    }
  }

  /**
   * Configure allocated evaluators with EM configuration and give them id numbers.
   */
  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    private final AtomicInteger activeEvaluatorCount = new AtomicInteger(0);

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final Configuration partialContextConf = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, CONTEXT_ID_PREFIX + activeEvaluatorCount.getAndIncrement())
          .build();

      final Configuration contextConf = Configurations.merge(
          partialContextConf, emConf.getContextConfiguration());

      final Configuration serviceConf = emConf.getServiceConfiguration();

      allocatedEvaluator.submitContextAndService(contextConf, serviceConf);
      LOG.info(activeEvaluatorCount.get() + " evaluators active!");
    }
  }

  /**
   * CmpTask-0 goes on CmpContext-0, and CmpTask-1 goes on CmpContext-1.
   */
  public final class ActiveContextHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(final ActiveContext activeContext) {
      final String contextId = activeContext.getId();
      final String taskId = contextId.replace(CONTEXT_ID_PREFIX, TASK_ID_PREFIX);

      final Configuration taskConf = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, taskId)
          .set(TaskConfiguration.TASK, CmpTask.class)
          .set(TaskConfiguration.ON_SEND_MESSAGE, CmpTaskReady.class)
          .build();

      activeContext.submitTask(taskConf);
    }
  }

  /**
   * When both tasks are ready, make the faster one send all of its data to the slower one.
   */
  public final class TaskMessageHandler implements EventHandler<TaskMessage> {
    private static final String DEFAULT_STRING = "DEFAULT";
    private AtomicReference<String> prevContextId = new AtomicReference<>(DEFAULT_STRING);

    @Override
    public void onNext(final TaskMessage taskMessage) {
      LOG.info("Received task message from " + taskMessage.getContextId());

      if (!prevContextId.compareAndSet(DEFAULT_STRING, taskMessage.getContextId())) {
        // second evaluator goes here
        LOG.info("Move data from " + taskMessage.getContextId() + " to " + prevContextId.get());
        emService.move(CmpTask.KEY, null, taskMessage.getContextId(), prevContextId.get());
      } else {
        // first evaluator goes this way
      }
    }
  }
}
