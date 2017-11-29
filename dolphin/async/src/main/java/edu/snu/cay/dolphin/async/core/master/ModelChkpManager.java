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
package edu.snu.cay.dolphin.async.core.master;

import edu.snu.cay.dolphin.async.DolphinParameters;
import edu.snu.cay.dolphin.async.JobLogger;
import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.AllocatedTable;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import edu.snu.cay.utils.CatchableExecutors;
import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Tracks and maintains checkpoints of model table(s) during training.
 * It's for restoring model snapshots from checkpoints and evaluating it after training.
 * Since this class is not thread-safe, be careful to access it concurrently (okay for the current use case).
 */
@NotThreadSafe
final class ModelChkpManager {
  private final JobLogger jobLogger;

  private final LinkedList<Future<String>[]> checkpointIdFutures = new LinkedList<>();

  private final String modelTableId;
  private final String inputTableId;

  private final InjectionFuture<ETMaster> etMasterFuture;
  private final InjectionFuture<JobMessageObserver> jobMessageObserverFuture;

  private final InjectionFuture<MasterSideMsgSender> msgSender;

  private final AtomicInteger workerCount = new AtomicInteger(0);
  private final AtomicInteger chkpCounter = new AtomicInteger(0);
  private final AtomicInteger restoreCounter = new AtomicInteger(0);

  private List<AllocatedExecutor> runningServers;
  private List<AllocatedExecutor> runningWorkers;

  private final ExecutorService executor = CatchableExecutors.newSingleThreadExecutor();

  @Inject
  private ModelChkpManager(final JobLogger jobLogger,
                           final InjectionFuture<ETMaster> etMasterFuture,
                           final InjectionFuture<MasterSideMsgSender> msgSender,
                           final InjectionFuture<JobMessageObserver> jobMessageObserverFuture,
                           @Parameter(DolphinParameters.ModelTableId.class) final String modelTableId,
                           @Parameter(DolphinParameters.InputTableId.class) final String inputTableId) {
    this.jobLogger = jobLogger;
    this.etMasterFuture = etMasterFuture;
    this.jobMessageObserverFuture = jobMessageObserverFuture;
    this.msgSender = msgSender;
    this.modelTableId = modelTableId;
    this.inputTableId = inputTableId;
  }

  /**
   * Set executors for evaluating a model.
   * Should be called before starting model evaluation.
   * It assumes that these entries does not change in model evaluation phase.
   * @param serverExecutors server executors
   * @param workerExecutors worker executors
   */
  void setExecutors(final List<AllocatedExecutor> serverExecutors,
                    final List<AllocatedExecutor> workerExecutors) {
    this.runningServers = serverExecutors;
    this.runningWorkers = workerExecutors;
  }

  /**
   * On a message from worker.
   * Workers send messages that they are ready to evaluate next model.
   * Evaluation of one model table is started when all runningWorkers are synchronized.
   * Manager restores a model table from the oldest checkpoint and lets runningWorkers evaluate the table.
   * When there's no more checkpoints, manager stops worker from evaluation.
   */
  void onWorkerMsg() {
    final int numWorkersSentMsg = workerCount.incrementAndGet();

    jobLogger.log(Level.INFO, "Msg from a worker. [{0} / {1}]", new Object[]{numWorkersSentMsg, runningWorkers.size()});

    if (numWorkersSentMsg == runningWorkers.size()) {
      workerCount.set(0); // reset
      executor.submit(() -> {
        final boolean doNext = restoreOldestCheckpoint();
        runningWorkers.forEach(worker -> msgSender.get().sendModelEvalAnsMsg(worker.getId(), doNext));
      });
    }
  }

  /**
   * Restores tables with the oldest checkpoint.
   * It waits until the restoration finishes.
   */
  private boolean restoreOldestCheckpoint() {
    // if it's the first restore wait until all chkps to be done
    if (restoreCounter.getAndIncrement() == 0) {
      waitChkpsToBeDone();
    }

    if (checkpointIdFutures.isEmpty()) {
      jobLogger.log(Level.INFO, "No more checkpoints.");
      return false;
    }

    jobMessageObserverFuture.get().sendMessageToClient(
        String.format("Model eval progress: [%d / %d]", restoreCounter.get(), chkpCounter.get()).getBytes());

    // Need to drop the previous model table first
    try {
      final Future future0 = etMasterFuture.get().getTable(inputTableId).drop();
      final Future future1 = etMasterFuture.get().getTable(modelTableId).drop();
      future0.get();
      future1.get();
    } catch (TableNotExistException | InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    // sleep before starting table chkp load
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // restore a model to evaluate from a checkpoint
    try {
      final Future<String>[] chkpIdFutures = checkpointIdFutures.pop();

      final String inputChkpId = chkpIdFutures[0].get();
      final String modelChkpId = chkpIdFutures[1].get();

      final Future<AllocatedTable> inputTableFuture = etMasterFuture.get().createTable(inputChkpId, runningWorkers);
      final Future<AllocatedTable> modelTableFuture = etMasterFuture.get().createTable(modelChkpId, runningServers);
      final AllocatedTable restoredModelTable = modelTableFuture.get();
      restoredModelTable.subscribe(runningWorkers).get();
      final AllocatedTable restoredInputTable = inputTableFuture.get();


      jobLogger.log(Level.INFO, "Table {0} and {1} are restored from checkpoint.",
          new Object[]{restoredModelTable.getId(), restoredInputTable.getId()});

    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    return true;
  }

  /**
   * Create a checkpoint of a current model table.
   */
  void createCheckpoint() {
    try {
      final ListenableFuture<String> inputChkpIdFuture = etMasterFuture.get().getTable(inputTableId).checkpoint();
      final ListenableFuture<String> modelChkpIdFuture = etMasterFuture.get().getTable(modelTableId).checkpoint();

      final int idx = chkpCounter.getAndIncrement();

      inputChkpIdFuture.addListener(chkpId ->
          jobLogger.log(Level.INFO, "{0}-th input checkpoint is created. Checkpoint Id: {1}",
              new Object[] {idx, chkpId}));
      modelChkpIdFuture.addListener(chkpId ->
          jobLogger.log(Level.INFO, "{0}-th model checkpoint is created. Checkpoint Id: {1}",
              new Object[]{idx, chkpId}));

      final Future[] chkpFuture = {inputChkpIdFuture, modelChkpIdFuture};
      checkpointIdFutures.add(chkpFuture);

    } catch (TableNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Waits all checkpoints requested by {@link #createCheckpoint()} to be done.
   */
  private void waitChkpsToBeDone() {
    checkpointIdFutures.forEach(futures -> {
      try {
        futures[0].get();
        futures[1].get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
    jobLogger.log(Level.INFO, "All checkpoints completed. NumCheckpoints: {0}", checkpointIdFutures.size());
  }
}
