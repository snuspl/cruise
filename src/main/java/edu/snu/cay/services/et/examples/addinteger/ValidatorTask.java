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
package edu.snu.cay.services.et.examples.addinteger;

import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import edu.snu.cay.services.et.examples.addinteger.parameters.*;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.services.et.examples.addinteger.AddIntegerETDriver.MODEL_TABLE_ID;

/**
 * The Validator Task that runs on Workers.
 * The expected result for each key is (numWorkers * numUpdates / numKeys), as each update increments the value by one.
 * An exception is thrown if the expected result is not found.
 */
@TaskSide
public final class ValidatorTask implements Task {
  private static final Logger LOG = Logger.getLogger(ValidatorTask.class.getName());

  private final TableAccessor tableAccessor;
  private final int startKey;
  private final int deltaValue;
  private final int updateCoefficient;
  private final int numKeys;
  private final int numWorkers;
  private final int numUpdates;

  @Inject
  private ValidatorTask(final TableAccessor tableAccessor,
                        @Parameter(StartKey.class) final int startKey,
                        @Parameter(DeltaValue.class) final int deltaValue,
                        @Parameter(UpdateCoefficient.class) final int updateCoefficient,
                        @Parameter(NumKeys.class) final int numKeys,
                        @Parameter(NumWorkers.class) final int numWorkers,
                        @Parameter(NumUpdates.class) final int numUpdates) {
    this.tableAccessor = tableAccessor;
    this.startKey = startKey;
    this.deltaValue = deltaValue;
    this.updateCoefficient = updateCoefficient;
    this.numKeys = numKeys;
    this.numWorkers = numWorkers;
    this.numUpdates = numUpdates;
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "Task.call() commencing...");

    final long sleepMillis = 100;
    int numRetries = 20;

    final int expectedResult = (numWorkers * numUpdates / numKeys) * (deltaValue * updateCoefficient);
    while (numRetries > 0) {
      numRetries--;

      try {
        if (validate(expectedResult)) {
          return null;
        }
      } catch (final IntegerValidationException e) {
        if (numRetries > 0) {
          LOG.log(Level.INFO, "Sleeping {0} ms to let it catch up.", sleepMillis);
          Thread.sleep(sleepMillis);
        } else {
          throw new RuntimeException(e);
        }
      }
    }
    return null;
  }

  private boolean validate(final int expectedResult) throws IntegerValidationException, TableNotExistException {
    final Table<Integer, Integer> modelTable = tableAccessor.get(MODEL_TABLE_ID);

    for (int i = 0; i < numKeys; i++) {
      final int key = startKey + i;
      final int result = modelTable.get(key);

      if (expectedResult != result) {
        LOG.log(Level.WARNING, "For key {0}, expected value {1} but received {2}",
            new Object[]{key, expectedResult, result});
        throw new IntegerValidationException(key, expectedResult, result);
      } else {
        LOG.log(Level.INFO, "For key {0}, received expected value {1}.", new Object[]{key, expectedResult});
      }
    }
    return true;
  }

  private static class IntegerValidationException extends Exception {
    IntegerValidationException(final int key, final int expected, final int actual) {
      super(String.format("For key %d, expected value %d but received %d", key, expected, actual));
    }
  }
}
