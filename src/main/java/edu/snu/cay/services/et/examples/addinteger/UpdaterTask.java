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
import edu.snu.cay.services.et.examples.addinteger.parameters.NumKeys;
import edu.snu.cay.services.et.examples.addinteger.parameters.NumUpdates;
import edu.snu.cay.services.et.examples.addinteger.parameters.StartKey;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.services.et.examples.addinteger.AddIntegerETDriver.MODEL_TABLE_ID;

/**
 * The UpdaterTask that runs on all Workers.
 * It runs updates that increment the value, numUpdates times,
 * in a round-robin order on keys in the range [startKey, startKey + numKeys).
 * At the last, it runs gets on this key range.
 * The result of the Get will be a _maximum_ of numUpdatesPerWorker * numWorkers (defined at Driver), i.e.
 * reflecting the global total of updates.
 * Some global updates may not have been processed yet. All updates will be accounted for in the ValidatorTask.
 */
@TaskSide
public final class UpdaterTask implements Task {
  private static final Logger LOG = Logger.getLogger(UpdaterTask.class.getName());

  private final TableAccessor tableAccessor;
  private final int startKey;
  private final int numKeys;
  private final int numUpdates;

  @Inject
  private UpdaterTask(final TableAccessor tableAccessor,
                      @Parameter(StartKey.class) final int startKey,
                      @Parameter(NumKeys.class) final int numKeys,
                      @Parameter(NumUpdates.class) final int numUpdates) {
    this.tableAccessor = tableAccessor;
    this.startKey = startKey;
    this.numKeys = numKeys;
    this.numUpdates = numUpdates;
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "Task.call() commencing...");
    final Table<Integer, Integer> modelTable = tableAccessor.get(MODEL_TABLE_ID);

    for (int i = 0; i < numUpdates; i++) {
      final int keyToUpdate = startKey + (i % numKeys);
      final Integer updatedValue = modelTable.update(keyToUpdate, 1);
      LOG.log(Level.INFO, "Update result for key {0}: {1}", new Object[]{keyToUpdate, updatedValue});
    }

    LOG.log(Level.INFO, "{0} updates complete", numUpdates);

    for (int k = 0; k < numKeys; k++) {
      final int key = startKey + k;
      final Integer getResult = modelTable.get(key);
      LOG.log(Level.INFO, "Get result for key {0}: {1}", new Object[]{key, getResult});
    }
    return null;
  }
}
