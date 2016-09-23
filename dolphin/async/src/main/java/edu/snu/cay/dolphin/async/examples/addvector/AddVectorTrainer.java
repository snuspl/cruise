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
import edu.snu.cay.dolphin.async.Trainer;
import edu.snu.cay.dolphin.async.metric.Tracer;
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link Trainer} class for the AddVectorREEF application.
 * Pushes a value to the server and checks the current value at the server via pull, once per iteration.
 * It sleeps {@link #computeTime} for each iteration to simulate computation, preventing the saturation of NCS of PS.
 */
final class AddVectorTrainer implements Trainer {
  private static final Logger LOG = Logger.getLogger(AddVectorTrainer.class.getName());

  /**
   * Sleep to wait validation check is possible.
   */
  private static final long VALIDATE_SLEEP_MS = 100;

  /**
   * Retry validation check maximum 20 times to wait server processing.
   */
  private static final int NUM_VALIDATE_RETRIES = 20;

  /**
   * The number of items to process. Currently AddVectorTrainer is assumed to process only 1 element at fixed cost.
   * We can extend it by storing data to process in its MemoryStore.
   */
  private static final int NUM_DATA_ITEMS_TO_PROCESS = 1;

  private final ParameterWorker<Integer, Integer, Vector> parameterWorker;

  /**
   * The integer to be added to each key in an update.
   */
  private final int delta;

  /**
   * a key list to use in communicating with PS server.
   */
  private final List<Integer> keyList;

  /**
   * Number of training data instances to be processed per mini-batch.
   */
  private final int miniBatchSize;

  // TODO #822: AddVector needs an actual training data set.
  private static final int numTotalInstances = 100;
  private final int numMiniBatches;

  /**
   * Sleep time to simulate computation.
   */
  private final long computeTime;

  /**
   * The expected total sum of each key.
   */
  private final int expectedResult;

  // TODO #734: Improve AddVector example to enable runtime optimization
  private final MemoryStore<Long> memoryStore;

  // TODO #487: Metric collecting should be done by the system, not manually by the user code.
  private final MetricsMsgSender<WorkerMetrics> metricsMsgSender;
  private final Tracer pushTracer;
  private final Tracer pullTracer;
  private final Tracer computeTracer;

  @Inject
  private AddVectorTrainer(final ParameterWorker<Integer, Integer, Vector> parameterWorker,
                           @Parameter(AddVectorREEF.DeltaValue.class) final int delta,
                           @Parameter(AddVectorREEF.NumKeys.class) final int numberOfKeys,
                           @Parameter(AddVectorREEF.NumWorkers.class) final int numberOfWorkers,
                           @Parameter(AddVectorREEF.ComputeTimeMs.class) final long computeTime,
                           @Parameter(Parameters.Iterations.class) final int numIterations,
                           @Parameter(Parameters.MiniBatchSize.class) final int miniBatchSize,
                           final MemoryStore<Long> memoryStore,
                           final MetricsMsgSender<WorkerMetrics> metricsMsgSender) {
    this.parameterWorker = parameterWorker;
    this.delta = delta;
    this.keyList = new ArrayList<>(numberOfKeys);
    for (int key = 0; key < numberOfKeys; key++) {
      keyList.add(key);
    }

    this.computeTime = computeTime;
    this.miniBatchSize = miniBatchSize;
    this.numMiniBatches = (int) Math.ceil((double) numTotalInstances / miniBatchSize);

    // TODO #681: Need to consider numWorkerThreads after multi-thread worker is enabled
    this.expectedResult = delta * numberOfWorkers * numIterations * numMiniBatches;
    LOG.log(Level.INFO, "delta:{0}, numWorkers:{1}, numIterations:{2}, numMiniBatchesPerItr:{3}",
        new Object[]{delta, numberOfWorkers, numIterations, numMiniBatches});

    this.memoryStore = memoryStore;
    this.metricsMsgSender = metricsMsgSender;

    this.pushTracer = new Tracer();
    this.pullTracer = new Tracer();
    this.computeTracer = new Tracer();
  }

  @Override
  public void initialize() {
  }

  @Override
  public void run(final int iteration) {
    final long iterationBegin = System.currentTimeMillis();
    resetTracers();

    // run mini-batches
    for (int miniBatchIdx = 0; miniBatchIdx < numMiniBatches; miniBatchIdx++) {
      // 1. pull model to compute with
      pullTracer.startTimer();
      final List<Vector> valueList = parameterWorker.pull(keyList);
      pullTracer.recordTime(valueList.size());
      LOG.log(Level.FINE, "Current values associated with keys {0} is {1}", new Object[]{keyList, valueList});

      // 2. sleep to simulate computation
      try {
        computeTracer.startTimer();
        Thread.sleep(computeTime);
      } catch (final InterruptedException e) {
        LOG.log(Level.WARNING, "Interrupted while sleeping to simulate computation", e);
      } finally {
        computeTracer.recordTime(NUM_DATA_ITEMS_TO_PROCESS);
      }

      // 3. push computed model
      pushTracer.startTimer();
      for (final int key : keyList) {
        parameterWorker.push(key, delta);
      }
      pushTracer.recordTime(keyList.size());
    }
    final double elapsedTime = (System.currentTimeMillis() - iterationBegin) / 1000.0D;
    // send empty metrics to trigger optimization
    final WorkerMetrics workerMetrics =
        buildMetricsMsg(iteration, memoryStore.getNumBlocks(), elapsedTime);

    LOG.log(Level.INFO, "WorkerMetrics {0}", workerMetrics);
    sendMetrics(workerMetrics);
  }

  private void sendMetrics(final WorkerMetrics workerMetrics) {
    LOG.log(Level.FINE, "Sending WorkerMetrics {0}", workerMetrics);

    metricsMsgSender.send(workerMetrics);
  }

  private WorkerMetrics buildMetricsMsg(final int iteration, final int numDataBlocks, final double elapsedTime) {
    return WorkerMetrics.newBuilder()
        .setEpochIdx(iteration)
        .setMiniBatchSize(miniBatchSize)
        .setNumMiniBatchForEpoch(numMiniBatches)
        .setProcessedDataItemCount(NUM_DATA_ITEMS_TO_PROCESS)
        .setNumDataBlocks(numDataBlocks)
        .setTotalTime(elapsedTime)
        .setTotalCompTime(computeTracer.totalElapsedTime())
        .setTotalPullTime(pullTracer.totalElapsedTime())
        .setAvgPullTime(pullTracer.avgTimePerRecord())
        .setTotalPushTime(pushTracer.totalElapsedTime())
        .setAvgPushTime(pushTracer.avgTimePerRecord())
        .setParameterWorkerMetrics(parameterWorker.buildParameterWorkerMetrics())
        .build();
  }

  @Override
  public void cleanup() {
    int numRemainingRetries = NUM_VALIDATE_RETRIES;

    while (numRemainingRetries-- > 0) {
      if (validate()) {
        LOG.log(Level.INFO, "Validation success");
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
   * @return true if all of the values of keyList are matched with expected result, otherwise false.
   */
  private boolean validate() {
    LOG.log(Level.INFO, "Start validation");
    boolean isSuccess = true;

    final List<Vector> valueList = parameterWorker.pull(keyList);
    LOG.log(Level.FINE, "Current values associated with keys {0} is {1}", new Object[]{keyList, valueList});

    for (int idx = 0; idx < keyList.size(); idx++) {
      final int key = keyList.get(idx);
      final Vector value = valueList.get(idx);

      if (expectedResult != value.get(0)) { // check only the first element, because all elements are identical
        LOG.log(Level.WARNING, "For key {0}, expected value of elements is {1} but received {2}",
            new Object[]{key, expectedResult, value});
        isSuccess = false;
      }
    }
    return isSuccess;
  }

  private void resetTracers() {
    pushTracer.resetTrace();
    pullTracer.resetTrace();
    computeTracer.resetTrace();
  }
}
