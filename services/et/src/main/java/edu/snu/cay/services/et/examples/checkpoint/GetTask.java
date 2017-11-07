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
package edu.snu.cay.services.et.examples.checkpoint;

import edu.snu.cay.services.et.configuration.parameters.ExecutorIdentifier;
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.services.et.examples.checkpoint.PutTask.NUM_ITEMS;
import static edu.snu.cay.services.et.examples.checkpoint.PutTask.VALUE_PREFIX;

/**
 * Task code that gets values from the table to confirm that the table has values put by {@link PutTask}.
 */
final class GetTask implements Task {
  private static final Logger LOG = Logger.getLogger(GetTask.class.getName());

  private final String executorId;

  private final double samplingRatio;

  private final TableAccessor tableAccessor;

  @Inject
  private GetTask(@Parameter(ExecutorIdentifier.class) final String executorId,
                  @Parameter(CheckpointETDriver.SamplingRatio.class) final double samplingRatio,
                  final TableAccessor tableAccessor) {
    this.executorId = executorId;
    this.samplingRatio = samplingRatio;
    this.tableAccessor = tableAccessor;
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "Hello! I am {0}", new Object[]{executorId});
    final Table<Long, String, ?> table = tableAccessor.getTable(CheckpointETDriver.TABLE_ID);

    int itemCount = 0;
    for (long key = 0; key < NUM_ITEMS; key++) {
      final String value = table.get(key).get();

      if (value != null) { // value can be null, if the checkpoint was done with sampling
        itemCount++;

        if (!value.equals(VALUE_PREFIX + key)) {
          throw new RuntimeException("key-value mapping is incorrect");
        }
      }
    }

    // check the number of items in a restored table
    if (samplingRatio == 1.0) {
      if (itemCount != NUM_ITEMS) {
        throw new RuntimeException(String.format(
            "Item count (%d) is different from what we expected (%d).", itemCount, NUM_ITEMS));
      }
    } else {
      // item count may be different from the expectation because sampling is done separately for each block
      LOG.log(Level.INFO, "Sampled items count:{0}, sampling rate: {1}, total: {2}",
          new Object[]{itemCount, samplingRatio, NUM_ITEMS});
    }

    LOG.log(Level.INFO, "Succeed to get {0} key-value items", itemCount);

    return null;
  }
}
