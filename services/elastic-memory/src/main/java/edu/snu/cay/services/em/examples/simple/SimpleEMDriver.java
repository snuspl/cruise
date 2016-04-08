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
import edu.snu.cay.services.em.avro.Result;
import edu.snu.cay.services.em.driver.impl.PartitionManager;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.driver.ElasticMemoryConfiguration;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.impl.RoundRobinDataIdFactory;
import edu.snu.cay.services.em.examples.simple.parameters.Iterations;
import edu.snu.cay.services.em.examples.simple.parameters.PeriodMillis;
import edu.snu.cay.utils.LongRangeUtils;
import edu.snu.cay.utils.trace.HTraceParameters;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.wake.IdentifierFactory;
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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver code for EMExample.
 * Two evaluators take turns moving half of their movable data to the other.
 * The number of iterations, and the period to wait between moves are configurable.
 */
@Unit
final class SimpleEMDriver {
  private static final Logger LOG = Logger.getLogger(SimpleEMDriver.class.getName());
  private static final int NUM_EVAL = 2;
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
          .setNumber(NUM_EVAL)
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

      final String contextId = CONTEXT_ID_PREFIX + evalCount;

      final Configuration partialContextConf = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, contextId)
          .build();

      final Configuration contextConf = Configurations.merge(
          partialContextConf, emConf.getContextConfiguration());

      final Configuration serviceConf = Configurations.merge(emConf.getServiceConfiguration(contextId, NUM_EVAL),
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
              .build());
      final Configuration traceConf = traceParameters.getConfiguration();

      final Configuration exampleConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(Iterations.class, Integer.toString(iterations))
          .bindNamedParameter(PeriodMillis.class, Long.toString(periodMillis))
          .build();

      allocatedEvaluator.submitContextAndService(contextConf,
          Configurations.merge(serviceConf, traceConf, exampleConf));
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
          .bindImplementation(DataIdFactory.class, RoundRobinDataIdFactory.class).build();

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
        final CountDownLatch transferredLatch = new CountDownLatch(1);
        final CountDownLatch finishedLatch = new CountDownLatch(1);

        final int initialSrcNumUnits = getNumUnits(srcId);
        final int initialDestNumUnits = getNumUnits(destId);
        final int numToMove = initialSrcNumUnits / 2;

        LOG.info("Move partitions of total size " + numToMove + " from " + srcId + " to " + destId);

        final boolean[] moveSucceeded = {false};

        try (final TraceScope moveTraceScope = Trace.startSpan("simpleMove", Sampler.ALWAYS)) {
          emService.move(SimpleEMTask.DATATYPE, numToMove, srcId, destId,
              new EventHandler<AvroElasticMemoryMessage>() {
                @Override
                public void onNext(final AvroElasticMemoryMessage emMsg) {
                  LOG.log(Level.INFO, "Move {0} data transfer completed.", emMsg.getOperationId());
                  transferredLatch.countDown();
                }
              },
              new EventHandler<AvroElasticMemoryMessage>() {
                @Override
                public void onNext(final AvroElasticMemoryMessage emMsg) {
                  moveSucceeded[0] = emMsg.getResultMsg().getResult().equals(Result.SUCCESS)
                      ? true : false;
                  LOG.log(Level.INFO, "Move {0} succeeded {1} with result {2}",
                      new Object[]{emMsg.getOperationId(), moveSucceeded[0],
                          emMsg.getResultMsg() == null ? "" : emMsg.getResultMsg().getResult()});
                  finishedLatch.countDown();
                }
              }
          );
        }

        // Wait for move to succeed
        try {
          LOG.log(Level.INFO, "Waiting for data transfers to finish on iteration {0}", i);
          transferredLatch.await();

          // Number of units should be unchanged until applyUpdates() is done.
          checkNumUnits(srcId, initialSrcNumUnits);
          checkNumUnits(destId, initialDestNumUnits);

          LOG.log(Level.INFO, "Applying updates on iteration {0}", i);
          emService.applyUpdates();

          // After update, number of units should be applied accordingly.
          checkNumUnits(srcId, initialSrcNumUnits - numToMove);
          checkNumUnits(destId, initialDestNumUnits + numToMove);

          LOG.log(Level.INFO, "Waiting for move to finish on iteration {0}", i);
          transferredLatch.await();

          if (moveSucceeded[0]) {
            LOG.log(Level.INFO, "Move finished on iteration {0}", i);
          } else {
            throw new RuntimeException("Move failed on iteration " + i);
          }
        } catch (final InterruptedException e) {
          throw new RuntimeException("Move wait interrupted on iteration " + i, e);
        }

        // Swap
        final String tmpContextId = srcId;
        srcId = destId;
        destId = tmpContextId;
      }
    }
  }

  private void checkNumUnits(final String evalId, final int expected) {
    final int actual = getNumUnits(evalId);
    if (actual != expected) {
      final String msg = new StringBuilder().append(evalId).append("should have ").append(expected)
          .append(", but has ").append(actual).append(" units").toString();
      throw new RuntimeException(msg);
    }
  }

  private int getNumUnits(final String evalId) {
    final Set<LongRange> rangeSet = partitionManager.getRangeSet(evalId, SimpleEMTask.DATATYPE);
    final int numUnits = (int) LongRangeUtils.getNumUnits(rangeSet);
    return numUnits;
  }
}
