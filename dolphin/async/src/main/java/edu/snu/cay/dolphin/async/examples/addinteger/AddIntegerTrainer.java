/*
 * Copyright (C) 2017 Seoul National University
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

import edu.snu.cay.dolphin.async.*;
import edu.snu.cay.dolphin.async.examples.common.ExampleParameters;
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link Trainer} class for the AddIntegerREEF application.
 * Pushes a value to the server and checks the current value at the server via pull, once per mini-batch.
 * It sleeps {@link #computeTime} for each mini-batch to simulate computation, preventing the saturation of NCS of PS.
 */
final class AddIntegerTrainer implements Trainer {
  private static final Logger LOG = Logger.getLogger(AddIntegerTrainer.class.getName());

  /**
   * Sleep to wait validation check is possible.
   */
  private static final long VALIDATE_SLEEP_MS = 100;

  /**
   * Retry validation check maximum 20 times to wait server processing.
   */
  private static final int NUM_VALIDATE_RETRIES = 20;

  private final ModelAccessor<Integer, Integer, Integer> modelAccessor;

  /**
   * The integer to be added to each key in an update.
   */
  private final int delta;

  /**
   * The number of keys.
   */
  private final int numberOfKeys;

  /**
   * Sleep time to simulate computation.
   */
  private final long computeTime;

  /**
   * The expected total sum of each key.
   */
  private final int expectedResult;

  @Inject
  private AddIntegerTrainer(final ModelAccessor<Integer, Integer, Integer> modelAccessor,
                            final TableAccessor tableAccessor,
                            @Parameter(DolphinParameters.InputTableId.class) final String inputTableId,
                            @Parameter(DolphinParameters.MaxNumEpochs.class) final int maxNumEpochs,
                            @Parameter(ExampleParameters.DeltaValue.class) final int delta,
                            @Parameter(ExampleParameters.NumKeys.class) final int numberOfKeys,
                            @Parameter(ExampleParameters.NumWorkers.class) final int numberOfWorkers,
                            @Parameter(ExampleParameters.ComputeTimeMs.class) final long computeTime,
                            @Parameter(ExampleParameters.NumTrainingData.class) final int numTrainingData)
      throws TableNotExistException {
    this.modelAccessor = modelAccessor;
    this.delta = delta;
    this.numberOfKeys = numberOfKeys;
    this.computeTime = computeTime;
    final int numMiniBatches = tableAccessor.getTable(inputTableId).getLocalTablet().getNumBlocks();

    this.expectedResult = delta * numberOfWorkers * maxNumEpochs * numMiniBatches;
    LOG.log(Level.INFO, "delta:{0}, numWorkers:{1}, maxNumEpochs:{2}, numTrainingData:{3}, numMiniBatches:{4}",
        new Object[]{delta, numberOfWorkers, maxNumEpochs, numTrainingData, numMiniBatches});
  }

  @Override
  public void initGlobalSettings() {
  }

  @Override
  public void runMiniBatch(final Collection miniBatchTrainingData) {
    try {
      Thread.sleep(computeTime * miniBatchTrainingData.size());
    } catch (final InterruptedException e) {
      LOG.log(Level.WARNING, "Interrupted while sleeping to simulate computation", e);
    }

    for (int key = 0; key < numberOfKeys; key++) {
      modelAccessor.push(key, delta);

      final Integer value = modelAccessor.pull(key);
      LOG.log(Level.INFO, "Current value associated with key {0} is {1}", new Object[]{key, value});
    }
  }

  @Override
  public EpochResult onEpochFinished(final Collection epochTrainingData, final Collection testData,
                                     final int epochIdx) {
    return EpochResult.EMPTY_RESULT;
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
   * @return true if all of the values of keys are matched with expected result, otherwise false.
   */
  private boolean validate() {
    LOG.log(Level.INFO, "Start validation");
    boolean isSuccess = true;
    for (int key = 0; key < numberOfKeys; key++) {
      final int result = modelAccessor.pull(key);

      if (expectedResult != result) {
        LOG.log(Level.WARNING, "For key {0}, expected value {1} but received {2}",
            new Object[]{key, expectedResult, result});
        isSuccess = false;
      }
    }
    return isSuccess;
  }
}
