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
package edu.snu.cay.dolphin.async.examples.addinteger;

import edu.snu.cay.common.metric.*;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.Worker;
import edu.snu.cay.dolphin.async.metric.Tracer;
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link Worker} class for the AddIntegerREEF application.
 * Pushes a value to the server and checks the current value at the server via pull, once per iteration.
 * It sleeps {@link #DELAY_MS} for each iteration to simulate computation, preventing the saturation of NCS of PS.
 */
final class AddIntegerWorker implements Worker {
  private static final Logger LOG = Logger.getLogger(AddIntegerWorker.class.getName());

  /**
   * Sleep 300 ms to simulate computation.
   */
  private static final long DELAY_MS = 300;

  /**
   * Sleep to wait validation check is possible.
   */
  private static final long VALIDATE_SLEEP_MS = 100;

  /**
   * Retry validation check maximum 20 times to wait server processing.
   */
  private static final int NUM_VALIDATE_RETRIES = 20;

  private final ParameterWorker<Integer, Integer, Integer> parameterWorker;

  /**
   * The integer to be added to each key in an update.
   */
  private final int delta;

  /**
   * The start key.
   */
  private final int startKey;

  /**
   * The number of keys.
   */
  private final int numberOfKeys;

  /**
   * The number of updates for each key in an iteration.
   */
  private final int numberOfUpdates;

  /**
   * The expected total sum of each key.
   */
  private final int expectedResult;

  private final MemoryStore<Long> memoryStore;

  // TODO #487: Metric collecting should be done by the system, not manually by the user code.
  private final MetricsMsgSender<WorkerMetrics> metricsMsgSender;

  private final Tracer pushTracer = new Tracer();
  private final Tracer pullTracer = new Tracer();
  private final Tracer computeTracer = new Tracer();

  /**
   * Number of iterations.
   */
  private int iteration = 0;

  @Inject
  private AddIntegerWorker(final ParameterWorker<Integer, Integer, Integer> parameterWorker,
                           @Parameter(AddIntegerREEF.DeltaValue.class) final int delta,
                           @Parameter(AddIntegerREEF.StartKey.class) final int startKey,
                           @Parameter(AddIntegerREEF.NumKeys.class) final int numberOfKeys,
                           @Parameter(AddIntegerREEF.NumUpdates.class) final int numberOfUpdates,
                           @Parameter(AddIntegerREEF.NumWorkers.class) final int numberOfWorkers,
                           @Parameter(Parameters.Iterations.class) final int numIterations,
                           final MemoryStore<Long> memoryStore,
                           final MetricsCollector metricsCollector,
                           final InsertableMetricTracker insertableMetricTracker,
                           final MetricsHandler metricsHandler,
                           final MetricsMsgSender<WorkerMetrics> metricsMsgSender) {
    this.parameterWorker = parameterWorker;
    this.delta = delta;
    this.startKey = startKey;
    this.numberOfKeys = numberOfKeys;
    this.numberOfUpdates = numberOfUpdates;
    // TODO #681: Need to consider numWorkerThreads after multi-thread worker is enabled
    this.expectedResult = delta * numberOfWorkers * numIterations * numberOfUpdates;
    LOG.log(Level.INFO, "delta:{0}, numWorkers:{1}, numIterations:{2}, numberOfUpdates:{3}",
        new Object[]{delta, numberOfWorkers, numIterations, numberOfUpdates});

    this.memoryStore = memoryStore;
    this.metricsMsgSender = metricsMsgSender;
  }

  @Override
  public void initialize() {
  }

  @Override
  public void run() {
    iteration++;
    final long iterationBegin = System.currentTimeMillis();

    resetTracers();

    // sleep to simulate computation
    computeTracer.startTimer();
    try {
      Thread.sleep(DELAY_MS);
    } catch (final InterruptedException e) {
      LOG.log(Level.WARNING, "Interrupted while sleeping to simulate computation", e);
    }
    computeTracer.recordTime(1);

    for (int i = 0; i < numberOfUpdates; i++) {
      for (int j = 0; j < numberOfKeys; j++) {
        pushTracer.startTimer();
        parameterWorker.push(startKey + j, delta);
        pushTracer.recordTime(1);

        pullTracer.startTimer();
        final Integer value = parameterWorker.pull(startKey + j);
        pullTracer.recordTime(1);
        LOG.log(Level.INFO, "Current value associated with key {0} is {1}", new Object[]{startKey + j, value});
      }
    }

    final double elapsedTime = (System.currentTimeMillis() - iterationBegin) / 1000.0D;
    final WorkerMetrics workerMetrics =
        buildMetricsMsg(memoryStore.getNumBlocks(), numberOfKeys, elapsedTime);

    sendMetrics(workerMetrics);
  }

  private void resetTracers() {
    pushTracer.resetTrace();
    pullTracer.resetTrace();
    computeTracer.resetTrace();
  }

  private void sendMetrics(final WorkerMetrics workerMetrics) {
    LOG.log(Level.FINE, "Sending WorkerMetrics {0}", workerMetrics);

    metricsMsgSender.send(workerMetrics);
  }

  private WorkerMetrics buildMetricsMsg(final int numDataBlocks,
                                        final int numProcessedDataItemCount, final double elapsedTime) {
    return WorkerMetrics.newBuilder()
        .setItrIdx(iteration)
        .setNumMiniBatchPerItr(1)
        .setNumDataBlocks(numDataBlocks)
        .setProcessedDataItemCount(numProcessedDataItemCount)
        .setTotalTime(elapsedTime)
        .setTotalCompTime(computeTracer.totalElapsedTime())
        .setTotalPullTime(pullTracer.totalElapsedTime())
        .setAvgPullTime(pullTracer.avgTimePerRecord())
        .setTotalPushTime(pushTracer.totalElapsedTime())
        .setAvgPushTime(pushTracer.avgTimePerRecord())
        .build();
  }

  @Override
  public void cleanup() {
    int numRetries = NUM_VALIDATE_RETRIES;

    while (numRetries-- > 0) {
      if (validate()) {
        return;
      }

      try {
        Thread.sleep(VALIDATE_SLEEP_MS);
      } catch (final InterruptedException e) {
        LOG.log(Level.WARNING, "Interrupted while sleeping to compare the result with expected value", e);
      }
    }

    LOG.log(Level.WARNING, "Validation failed");
  }

  /**
   * Checks the result(total sum) of each key is same with expected result.
   *
   * @return true if all of the values of keys are matched with expected result, otherwise false.
   */
  private boolean validate() {
    LOG.log(Level.INFO, "Start validation");
    boolean ret = true;
    for (int i = 0; i < numberOfKeys; i++) {
      final int result = parameterWorker.pull(startKey + i);

      if (expectedResult != result) {
        LOG.log(Level.WARNING, "For key {0}, expected value {1} but received {2}",
            new Object[]{startKey + i, expectedResult, result});
        ret = false;
      }
    }
    return ret;
  }
}
