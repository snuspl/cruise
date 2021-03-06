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
package edu.snu.spl.cruise.ps.core.master;

import edu.snu.spl.cruise.ps.CruisePSParameters;
import edu.snu.spl.cruise.ps.JobLogger;
import edu.snu.spl.cruise.ps.ProgressMsg;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * A component to track minibatch-progress by workers.
 * Different from {@link ProgressTracker}, it receives a progress message for every batch by workers.
 * And every {@link CruisePSParameters.NumTotalMiniBatches} batches,
 * it checkpoints a model table for offline model evaluation.
 */
public final class BatchProgressTracker {
  private final JobLogger jobLogger;

  private final ModelChkpManager modelChkpManager;

  private final int totalMiniBatchesToRun;
  private final int numMiniBatchesInEpoch;

  private final boolean offlineModelEval;

  private final Map<String, Integer> workerIdToBatchProgress = new ConcurrentHashMap<>();

  private final AtomicInteger miniBatchCounter = new AtomicInteger(0);

  @Inject
  private BatchProgressTracker(final JobLogger jobLogger,
                               @Parameter(CruisePSParameters.MaxNumEpochs.class) final int numEpochs,
                               @Parameter(CruisePSParameters.NumTotalMiniBatches.class)
                                 final int numMiniBatchesInEpoch,
                               @Parameter(CruisePSParameters.OfflineModelEvaluation.class)
                                 final boolean offlineModelEval,
                               final ModelChkpManager modelChkpManager) {
    this.jobLogger = jobLogger;
    this.modelChkpManager = modelChkpManager;
    this.totalMiniBatchesToRun = numEpochs * numMiniBatchesInEpoch;
    this.numMiniBatchesInEpoch = numMiniBatchesInEpoch;
    this.offlineModelEval = offlineModelEval;
  }

  synchronized void onProgressMsg(final ProgressMsg msg) {
    final String workerId = msg.getExecutorId().toString();
    final int miniBatchIdx = miniBatchCounter.incrementAndGet();
    jobLogger.log(Level.INFO, "Batch progress: {0} / {1}.",
        new Object[]{miniBatchIdx, totalMiniBatchesToRun});

    if (offlineModelEval) {
      if (miniBatchIdx % numMiniBatchesInEpoch == 0) {
        jobLogger.log(Level.INFO, "Checkpoint model table. EpochIdx: {0}", miniBatchIdx / numMiniBatchesInEpoch);
        modelChkpManager.createCheckpoint();
      }
    }

    workerIdToBatchProgress.compute(workerId, (id, batchCount) -> batchCount == null ? 1 : batchCount + 1);
    jobLogger.log(Level.INFO, "Committed Batches per workers: {0}", workerIdToBatchProgress);
  }
}
