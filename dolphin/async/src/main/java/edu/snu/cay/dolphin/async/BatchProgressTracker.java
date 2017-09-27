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
package edu.snu.cay.dolphin.async;

import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A component to track minibatch-progress by workers.
 * Different from {@link ProgressTracker}, it receives a progress message for every batch by workers.
 * And every {@link edu.snu.cay.dolphin.async.DolphinParameters.NumTotalMiniBatches} batches,
 * it checkpoints a model table for offline model evaluation.
 */
public final class BatchProgressTracker {
  private static final Logger LOG = Logger.getLogger(BatchProgressTracker.class.getName());

  private final ModelChkpManager modelChkpManager;

  private final int numEpochs;
  private final int numMiniBatchesInEpoch;

  private final Map<String, Integer> workerIdToBatchProgress = new ConcurrentHashMap<>();

  private final AtomicInteger miniBatchCounter = new AtomicInteger(0);

  @Inject
  private BatchProgressTracker(@Parameter(DolphinParameters.MaxNumEpochs.class) final int numEpochs,
                               @Parameter(DolphinParameters.NumTotalMiniBatches.class)
                                 final int numMiniBatchesInEpoch,
                               final ModelChkpManager modelChkpManager) {
    this.modelChkpManager = modelChkpManager;
    this.numEpochs = numEpochs;
    this.numMiniBatchesInEpoch = numMiniBatchesInEpoch;
  }

  synchronized void onProgressMsg(final ProgressMsg msg) {
    final String workerId = msg.getExecutorId().toString();
    final int totalMiniBatchCount = miniBatchCounter.getAndIncrement();

    final int epochIdx = totalMiniBatchCount / numMiniBatchesInEpoch;
    final int batchIdx = totalMiniBatchCount % numMiniBatchesInEpoch;

    if (batchIdx == 0 && epochIdx > 0) {
      modelChkpManager.createCheckpoint();
    }
    workerIdToBatchProgress.compute(workerId, (id, batchCount) -> batchCount == null ? 1 : batchCount + 1);

    LOG.log(Level.INFO, "Epoch started: {0} / {1}. Batch started: {2} / {3}.",
        new Object[]{epochIdx + 1, numEpochs, batchIdx + 1, numMiniBatchesInEpoch});
    LOG.log(Level.INFO, "Committed Batches per workers: {0}", workerIdToBatchProgress);
  }
}