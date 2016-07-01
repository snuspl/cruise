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

import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.Worker;
import edu.snu.cay.dolphin.async.WorkerSynchronizer;
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
   * Synchronization component for setting a global barrier across workers.
   */
  private final WorkerSynchronizer synchronizer;

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

  @Inject
  private AddIntegerWorker(final ParameterWorker<Integer, Integer, Integer> parameterWorker,
                           final WorkerSynchronizer synchronizer,
                           @Parameter(AddIntegerREEF.DeltaValue.class) final int delta,
                           @Parameter(AddIntegerREEF.StartKey.class) final int startKey,
                           @Parameter(AddIntegerREEF.NumberOfKeys.class) final int numberOfKeys,
                           @Parameter(AddIntegerREEF.NumberOfUpdates.class) final int numberOfUpdates,
                           @Parameter(AddIntegerREEF.NumberOfWorkers.class) final int numberOfWorkers,
                           @Parameter(Parameters.NumWorkerThreads.class) final int numWorkerThreads,
                           @Parameter(Parameters.Iterations.class) final int numIterations) {
    this.parameterWorker = parameterWorker;
    this.synchronizer = synchronizer;
    this.delta = delta;
    this.startKey = startKey;
    this.numberOfKeys = numberOfKeys;
    this.numberOfUpdates = numberOfUpdates;
    this.expectedResult = delta * numberOfWorkers * numWorkerThreads * numIterations * numberOfUpdates;
    LOG.log(Level.INFO, "delta:{0}, numWorkers:{1}, numWorkerThreads:{2}, numIterations:{3}, numberOfUpdates:{4}",
        new Object[]{delta, numberOfWorkers, numWorkerThreads, numIterations, numberOfUpdates});
  }

  @Override
  public void initialize() {
    // all of the workers should start at the same time to use it as a benchmark
    synchronizer.globalBarrier();
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
      Integer value = 0;
      for (int j = 0; j < numberOfKeys; j++) {
        parameterWorker.push(startKey + j, delta);
        value = parameterWorker.pull(startKey + j);
      }
      LOG.log(Level.INFO, "Current value associated with key {0} is {1}", new Object[]{startKey + i, value});
    }
  }

  @Override
  public void cleanup() {
    // validation check is possible when all of the workers finish their jobs
    // so, barrier is used to wait other workers
    synchronizer.globalBarrier();

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

    LOG.log(Level.WARNING, "Validation test is failed");
    throw new RuntimeException();
  }

  /**
   * check the result(total sum) of each key is same with expected result.
   *
   * @return true if all of the values of keys are matched with expected result, otherwise false.
   */
  private boolean validate() {
    for (int i = 0; i < numberOfKeys; i++) {
      final int result = parameterWorker.pull(startKey + i);

      if (expectedResult != result) {
        LOG.log(Level.WARNING, "For key {0}, expected value {1} but received {2}",
            new Object[]{startKey + i, expectedResult, result});
        return false;
      } else {
        LOG.log(Level.INFO, "For key {0}, received expected value {1}.", new Object[]{startKey + i, expectedResult});
      }
    }
    return true;
  }
}
