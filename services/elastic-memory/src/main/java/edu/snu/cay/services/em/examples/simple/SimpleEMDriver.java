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

import edu.snu.cay.services.em.avro.AvroElasticMemoryMessage;
import edu.snu.cay.services.em.avro.UpdateResult;
import edu.snu.cay.services.em.driver.PartitionManager;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.driver.ElasticMemoryConfiguration;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.impl.BaseCounterDataIdFactory;
import edu.snu.cay.services.em.examples.simple.parameters.Iterations;
import edu.snu.cay.services.em.examples.simple.parameters.PeriodMillis;
import edu.snu.cay.services.em.trace.HTraceParameters;
import org.apache.commons.lang.math.LongRange;
import org.htrace.Sampler;
import org.htrace.Trace;
import org.htrace.TraceScope;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.driver.task.TaskMessage;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Parameter;
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
 * Driver code for EMExample.
 * Two evaluators take turns moving half of their movable data to the other.
 * The number of iterations, and the period to wait between moves are configurable.
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
  private final int iterations;
  private final long periodMillis;

  @Inject
  private SimpleEMDriver(final EvaluatorRequestor requestor,
                         final ElasticMemoryConfiguration emConf,
                         final ElasticMemory emService,
                         final PartitionManager partitionManager,
                         final HTraceParameters traceParameters,
                         @Parameter(Iterations.class) final int iterations,
                         @Parameter(PeriodMillis.class) final long periodMillis) throws InjectionException {
    this.requestor = requestor;
    this.emConf = emConf;
    this.emService = emService;
    this.partitionManager = partitionManager;
    this.traceParameters = traceParameters;
    this.iterations = iterations;
    this.periodMillis = periodMillis;
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

      final Configuration exampleConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(Iterations.class, Integer.toString(iterations))
          .bindNamedParameter(PeriodMillis.class, Long.toString(periodMillis))
          .build();

      allocatedEvaluator.submitContextAndService(contextConf,
          Configurations.merge(emServiceConf, traceConf, exampleConf));
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

      final Configuration idFactoryConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindImplementation(DataIdFactory.class, BaseCounterDataIdFactory.class)
          .bindNamedParameter(BaseCounterDataIdFactory.Base.class,
              taskId.substring(SimpleEMDriver.TASK_ID_PREFIX.length())).build();

      final Configuration taskConf = Configurations.merge(
          TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, taskId)
              .set(TaskConfiguration.TASK, SimpleEMTask.class)
              .set(TaskConfiguration.ON_SEND_MESSAGE, SimpleEMTaskReady.class)
              .build(),
          idFactoryConf);

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
        final String slowId = taskMessage.getContextId();
        final String fastId = prevContextId.get();
        runMoves(slowId, fastId);
      } else {
        // fast evaluator goes this way
      }
    }

    private void runMoves(final String firstContextId, final String secondContextId) {

      String srcId = firstContextId;
      String destId = secondContextId;

      for (int i = 0; i < iterations; i++) {

        final Set<LongRange> srcRangeSet = partitionManager.getRangeSet(srcId, SimpleEMTask.DATATYPE);

        final long numToMove = getNumUnits(srcRangeSet) / 2;
        LOG.info("Move partitions of total size " + numToMove + " from " + srcId + " to " + destId);

        final Set<LongRange> rangeSetToMove = getRangeSetToMove(srcId, destId, srcRangeSet, numToMove);
        for (final LongRange range : rangeSetToMove) {
          LOG.info("- " + range + ", size: " + (range.getMaximumLong() - range.getMinimumLong() + 1));
        }

        final boolean[] moveSucceeded = {false};

        try (final TraceScope moveTraceScope = Trace.startSpan("simpleMove", Sampler.ALWAYS)) {
          emService.move(SimpleEMTask.DATATYPE, rangeSetToMove, srcId, destId,
              new EventHandler<AvroElasticMemoryMessage>() {
                @Override
                public void onNext(final AvroElasticMemoryMessage emMsg) {
                  synchronized (SimpleEMDriver.this) {
                    moveSucceeded[0] = emMsg.getUpdateAckMsg().getResult().equals(UpdateResult.SENDER_UPDATED)
                        ? true : false;
                    LOG.info("Move " + emMsg.getOperationId() + (moveSucceeded[0]
                        ? " succeeded" : " " + emMsg.getUpdateAckMsg().getResult()));
                    SimpleEMDriver.this.notifyAll();
                  }
                }
              }
          );
        }

        // Swap
        final String tmpContextId = srcId;
        srcId = destId;
        destId = tmpContextId;

        // Wait for move to succeed
        synchronized (this) {
          try {
            wait(periodMillis);
            if (!moveSucceeded[0]) {
              throw new RuntimeException("Move failed on iteration " + i);
            }
          } catch (final InterruptedException e) {
            throw new RuntimeException("Move wait interrupted on iteration " + i, e);
          }
        }
      }
    }

    private long getNumUnits(final Set<LongRange> rangeSet) {
      long numUnits = 0;
      for (final LongRange idRange : rangeSet) {
        numUnits += (idRange.getMaximumLong() - idRange.getMinimumLong() + 1);
      }
      return numUnits;
    }

    /**
     * Get RangeSet to move, and in the process splitting and registering
     * the partitions across src and dest via the partitionManager.
     *
     * For example, to move partitions of total size 2 from a src with a single partition [1, 3],
     * this method will unregister [1, 3] then split up and register [3, 3] at src and [1, 2] at dst.
     * It will return the set with [1, 2].
     */
    private Set<LongRange> getRangeSetToMove(final String srcId, final String destId,
                                             final Set<LongRange> currentRangeSet, final long totalToMove) {
      try (final TraceScope getRangeTraceScope = Trace.startSpan("getRangeSetToMove", Sampler.ALWAYS)) {

        final Set<LongRange> rangeSetToMove = new HashSet<>();
        long remaining = totalToMove;

        for (final LongRange idRange : currentRangeSet) {
          final long length = idRange.getMaximumLong() - idRange.getMinimumLong() + 1;
          final long numToMove = remaining > length ? length : remaining;
          remaining -= numToMove;

          if (numToMove == length) { // Move the whole range
            partitionManager.remove(srcId, SimpleEMTask.DATATYPE, idRange);
            partitionManager.register(destId, SimpleEMTask.DATATYPE, idRange);
            rangeSetToMove.add(idRange);
          } else { // Split the range and move it
            partitionManager.remove(srcId, SimpleEMTask.DATATYPE, idRange);
            final long lastIdToKeep = idRange.getMaximumLong() - numToMove;
            final LongRange rangeToKeep = new LongRange(idRange.getMinimumLong(), lastIdToKeep);
            final LongRange rangeToMove = new LongRange(lastIdToKeep + 1, idRange.getMaximumLong());
            partitionManager.register(srcId, SimpleEMTask.DATATYPE, rangeToKeep);
            partitionManager.register(destId, SimpleEMTask.DATATYPE, rangeToMove);
            rangeSetToMove.add(rangeToMove);
          }

          if (remaining == 0) {
            break;
          }
        }

        if (remaining > 0) {
          LOG.warning("Tried to move partitions of total size " + totalToMove +
              ", but only found " + (totalToMove - remaining) + " partitions to move");
        }

        return rangeSetToMove;

      }
    }
  }
}
