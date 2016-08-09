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
package edu.snu.cay.dolphin.async.examples.addvector;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.metric.MetricsMsgSender;
import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.Worker;
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
final class AddVectorWorker implements Worker {
  private static final Logger LOG = Logger.getLogger(AddVectorWorker.class.getName());

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

  private final ParameterWorker<Integer, Integer, Vector> parameterWorker;

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

  @Inject
  private AddVectorWorker(final ParameterWorker<Integer, Integer, Vector> parameterWorker,
                          @Parameter(AddVectorREEF.DeltaValue.class) final int delta,
                          @Parameter(AddVectorREEF.StartKey.class) final int startKey,
                          @Parameter(AddVectorREEF.NumKeys.class) final int numberOfKeys,
                          @Parameter(AddVectorREEF.NumUpdates.class) final int numberOfUpdates,
                          @Parameter(AddVectorREEF.NumWorkers.class) final int numberOfWorkers,
                          @Parameter(Parameters.Iterations.class) final int numIterations,
                          final MemoryStore<Long> memoryStore,
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
    // sleep to simulate computation
    try {
      Thread.sleep(DELAY_MS);
    } catch (final InterruptedException e) {
      LOG.log(Level.WARNING, "Interrupted while sleeping to simulate computation", e);
    }

    for (int i = 0; i < numberOfUpdates; i++) {
      for (int j = 0; j < numberOfKeys; j++) {
        parameterWorker.push(startKey + j, delta);
        final Vector value = parameterWorker.pull(startKey + j);
        LOG.log(Level.INFO, "Current value associated with key {0} is {1}", new Object[]{startKey + j, value});
      }
    }

    // send empty metrics to trigger optimization
    final WorkerMetrics workerMetrics =
        buildMetricsMsg(memoryStore.getNumBlocks());

    sendMetrics(workerMetrics);
  }

  private void sendMetrics(final WorkerMetrics workerMetrics) {
    LOG.log(Level.FINE, "Sending WorkerMetrics {0}", workerMetrics);

    metricsMsgSender.send(workerMetrics);
  }

  private WorkerMetrics buildMetricsMsg(final int numDataBlocks) {
    return WorkerMetrics.newBuilder()
        .setNumDataBlocks(numDataBlocks)
        .build();
  }

  @Override
  public void cleanup() {
    int numRetries = NUM_VALIDATE_RETRIES;

    while (numRetries-- > 0) {
      if (validate()) {
        LOG.log(Level.WARNING, "Validation success");
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
    boolean isSuccess = true;
    for (int i = 0; i < numberOfKeys; i++) {
      final Vector result = parameterWorker.pull(startKey + i);

      if (expectedResult != result.get(0)) { // check only the first element
        LOG.log(Level.WARNING, "For key {0}, expected value of elements is {1} but received {2}",
            new Object[]{startKey + i, expectedResult, result});
        isSuccess = false;
      }
    }
    return isSuccess;
  }
}
