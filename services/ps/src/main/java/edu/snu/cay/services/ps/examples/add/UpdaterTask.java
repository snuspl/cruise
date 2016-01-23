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
package edu.snu.cay.services.ps.examples.add;

import edu.snu.cay.services.ps.examples.add.parameters.NumKeys;
import edu.snu.cay.services.ps.examples.add.parameters.StartKey;
import edu.snu.cay.services.ps.examples.add.parameters.NumUpdates;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Updater Task that runs on all Workers.
 * We run push updates that increment the value,
 * numUpdates times (passed in from numUpdatesPerWorker on the Driver),
 * in a round-robin order on keys in the range [startKey, startKey + numKeys).
 * We then run pull on this key range.
 * The result of the pull will be a _maximum_ of numUpdatesPerWorker * numWorkers (defined at Driver), i.e.
 * reflecting the global total of updates.
 * Some global updates may not have been processed yet. All updates will be accounted for in the ValidatorTask.
 */
@TaskSide
public final class UpdaterTask implements Task {
  private static final Logger LOG = Logger.getLogger(UpdaterTask.class.getName());

  private final ParameterWorker<Integer, Integer, Integer> worker;
  private final int startKey;
  private final int numKeys;
  private final int numUpdates;

  @Inject
  private UpdaterTask(final ParameterWorker<Integer, Integer, Integer> worker,
                      @Parameter(StartKey.class) final int startKey,
                      @Parameter(NumKeys.class) final int numKeys,
                      @Parameter(NumUpdates.class) final int numUpdates) {
    this.worker = worker;
    this.startKey = startKey;
    this.numKeys = numKeys;
    this.numUpdates = numUpdates;
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "Task.call() commencing...");
    final int loggingInterval = numUpdates / 4;

    for (int i = 0; i < numUpdates; i++) {
      if (i % loggingInterval == 0) {
        LOG.log(Level.INFO, "{0} updates complete", i);
        pullAllKeys();
      }
      worker.push(startKey + (i % numKeys), 1);
    }

    LOG.log(Level.INFO, "{0} updates complete", numUpdates);
    pullAllKeys();
    return null;
  }

  public void pullAllKeys() {
    for (int i = 0; i < numKeys; i++) {
      final int pullResult = worker.pull(startKey + i);
      LOG.log(Level.INFO, "Pull result for key {0}: {1}", new Object[]{startKey + i, pullResult});
    }
  }
}
