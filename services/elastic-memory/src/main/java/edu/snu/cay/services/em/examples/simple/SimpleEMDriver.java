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
package edu.snu.cay.services.em.examples.simple;

import edu.snu.cay.services.em.driver.PartitionManager;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.driver.ElasticMemoryConfiguration;
import edu.snu.cay.services.em.trace.HTraceParameters;
import org.apache.commons.lang.math.LongRange;
import org.apache.htrace.Sampler;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * Driver code for EMExample
 */
@Unit
final class SimpleEMDriver {
  private static final Logger LOG = Logger.getLogger(SimpleEMDriver.class.getName());
  private static final String CONTEXT_ID_PREFIX = "Context-";
  public static final String TASK_ID_PREFIX = "Task-";

  private final EvaluatorRequestor requestor;
  private final ElasticMemoryConfiguration emConf;
  private final ElasticMemory emService;
  private final PartitionManager partitionManager;
  private final HTraceParameters traceParameters;

  @Inject
  private SimpleEMDriver(final EvaluatorRequestor requestor,
                         final ElasticMemoryConfiguration emConf,
                         final ElasticMemory emService,
                         final PartitionManager partitionManager,
                         final HTraceParameters traceParameters) throws InjectionException {
    this.requestor = requestor;
    this.emConf = emConf;
    this.emService = emService;
    this.partitionManager = partitionManager;
    this.traceParameters = traceParameters;
  }

  /**
   * Spawn two small containers.
   */
  final class DriverStartHandler implements EventHandler<StartTime> {
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
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    private final AtomicInteger activeEvaluatorCount = new AtomicInteger(0);

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final int evalCount = activeEvaluatorCount.getAndIncrement();

      final Configuration partialContextConf = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, CONTEXT_ID_PREFIX + evalCount)
          .build();

      final Configuration contextConf = Configurations.merge(
          partialContextConf, emConf.getContextConfiguration());

      final Configuration emServiceConf = emConf.getServiceConfiguration();

      final Configuration traceConf = traceParameters.getConfiguration();

      allocatedEvaluator.submitContextAndService(contextConf, Configurations.merge(emServiceConf, traceConf));
      LOG.info((evalCount + 1) + " evaluators active!");
    }
  }

  /**
   * Task-0 goes on Context-0, and Task-1 goes on CmpContext-1.
   */
  final class ActiveContextHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(final ActiveContext activeContext) {
      final String contextId = activeContext.getId();
      final String taskId = contextId.replace(CONTEXT_ID_PREFIX, TASK_ID_PREFIX);

      final Configuration taskConf = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, taskId)
          .set(TaskConfiguration.TASK, SimpleEMTask.class)
          .set(TaskConfiguration.ON_SEND_MESSAGE, SimpleEMTaskReady.class)
          .build();

      activeContext.submitTask(taskConf);
    }
  }

  /**
   * When both tasks are ready, make the faster one send all of its data to the slower one.
   */
  final class TaskMessageHandler implements EventHandler<TaskMessage> {
    private static final String DEFAULT_STRING = "DEFAULT";
    private AtomicReference<String> prevContextId = new AtomicReference<>(DEFAULT_STRING);

    @Override
    public void onNext(final TaskMessage taskMessage) {
      LOG.info("Received task message from " + taskMessage.getContextId());

      if (!prevContextId.compareAndSet(DEFAULT_STRING, taskMessage.getContextId())) {
        // slow evaluator goes through here
        final String srcId = taskMessage.getContextId();
        final String destId = prevContextId.get();
        LOG.info("Move data from " + srcId + " to " + destId);

        final TraceScope moveTraceScope = Trace.startSpan("simpleMove", Sampler.ALWAYS);
        try {
          final Set<LongRange> oldIdRangeSet = partitionManager.getRangeSet(srcId, SimpleEMTask.KEY);
          final Set<LongRange> sendIdRangeSet = new HashSet<>();

          for (final LongRange idRange : oldIdRangeSet) {
            final long midId = (idRange.getMaximumLong() + idRange.getMinimumLong()) / 2;
            final LongRange lowIdRange = new LongRange(idRange.getMinimumLong(), midId);
            final LongRange highIdRange = new LongRange(midId + 1, idRange.getMaximumLong());
            partitionManager.remove(srcId, SimpleEMTask.KEY, idRange);
            partitionManager.registerPartition(srcId, SimpleEMTask.KEY, lowIdRange);
            partitionManager.registerPartition(destId, SimpleEMTask.KEY, highIdRange);
            sendIdRangeSet.add(highIdRange);
          }

          emService.move(SimpleEMTask.KEY, sendIdRangeSet, taskMessage.getContextId(), prevContextId.get());
        } finally {
          moveTraceScope.close();
        }

      } else {
        // first evaluator goes this way
      }
    }
  }
}
