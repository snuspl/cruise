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

import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.AllocatedTable;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import edu.snu.cay.utils.CatchableExecutors;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tracks and maintains checkpoints of model table(s).
 * Since this class is not thread-safe, be careful to access it concurrently (okay for the current use case).
 */
@NotThreadSafe
final class ModelChkpManager {
  private static final Logger LOG = Logger.getLogger(ModelChkpManager.class.getName());

  private final LinkedList<Future<String>> checkpointIdFutures = new LinkedList<>();

  private final String modelTableId;

  private final InjectionFuture<ETMaster> etMasterFuture;

  private final InjectionFuture<ETTaskRunner> etTaskRunnerFuture;

  private final InjectionFuture<MasterSideMsgSender> msgSender;

  private final AtomicInteger workerCount = new AtomicInteger(0);
  private final AtomicInteger chkpCounter = new AtomicInteger(0);
  private final AtomicBoolean restoreStarted = new AtomicBoolean(false);

  private final ExecutorService executor = CatchableExecutors.newSingleThreadExecutor();

  @Inject
  private ModelChkpManager(final InjectionFuture<ETMaster> etMasterFuture,
                                 final InjectionFuture<ETTaskRunner> etTaskRunnerFuture,
                                 final InjectionFuture<MasterSideMsgSender> msgSender,
                                 @Parameter(DolphinParameters.ModelTableId.class) final String modelTableId) {
    this.etMasterFuture = etMasterFuture;
    this.etTaskRunnerFuture = etTaskRunnerFuture;
    this.msgSender = msgSender;
    this.modelTableId = modelTableId;
  }

  /**
   * @return {@code True} if there are remaining checkpoints to consume.
   */
  boolean hasCheckpoints() {
    return !checkpointIdFutures.isEmpty();
  }

  /**
   *
   */
  void onWorkerMsg() {
    LOG.log(Level.INFO, "Msg from a worker");
    final List<AllocatedExecutor> workers = etTaskRunnerFuture.get().getWorkerExecutors();
    if (workerCount.incrementAndGet() == workers.size()) {
      workerCount.set(0); // reset
      executor.submit(() -> {
        final boolean doNext = restoreOldestCheckpoint();
        workers.forEach(worker -> msgSender.get().sendModelEvalAnsMsg(worker.getId(), doNext));
      });
    }
  }

  /**
   * Restores tables with the oldest checkpoint. This method is called only when {@link #hasCheckpoints()} is true.
   */
  private boolean restoreOldestCheckpoint() {
    if (!restoreStarted.getAndSet(true)) {
      // if it's the first restore
      waitChkpsToBeDone();
    }

    if (checkpointIdFutures.isEmpty()) {
      LOG.log(Level.INFO, "No more checkpoints.");
      return false;
    }

    // Need to drop the previous model table first
    try {
      etMasterFuture.get().getTable(modelTableId).drop().get();
    } catch (TableNotExistException | InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    // restore a model to evaluate from a checkpoint
    final String checkpointId;
    try {
      checkpointId = checkpointIdFutures.pop().get();

      final List<AllocatedExecutor> runningServers = new ArrayList<>(etTaskRunnerFuture.get().getServerExecutors());
      final List<AllocatedExecutor> runningWorvers = new ArrayList<>(etTaskRunnerFuture.get().getWorkerExecutors());

      final AllocatedTable restoredTable = etMasterFuture.get().createTable(checkpointId, runningServers).get();
      restoredTable.subscribe(runningWorvers).get();
      LOG.log(Level.INFO, "Table {0} is restored from checkpoint.", restoredTable.getId());

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
      final ListenableFuture<String> chkpIdFuture = etMasterFuture.get().getTable(modelTableId).checkpoint();
      final int idx = chkpCounter.getAndIncrement();
      chkpIdFuture.addListener(chkpId ->
          LOG.log(Level.INFO, "{0}-th checkpoint is created. Checkpoint Id: {1}", new Object[] {idx, chkpId}));
      checkpointIdFutures.add(chkpIdFuture);

    } catch (TableNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Waits all checkpoints requested by {@link #createCheckpoint()} to be done.
   */
  private void waitChkpsToBeDone() {
    checkpointIdFutures.forEach(future -> {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
    LOG.log(Level.INFO, "All checkpoints completed. NumCheckpoints: {0}", checkpointIdFutures.size());
  }
}
