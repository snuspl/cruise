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
package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.common.parameters.AddedEval;
import edu.snu.cay.services.em.common.parameters.MemoryStoreId;
import edu.snu.cay.services.em.common.parameters.NumTotalBlocks;
import edu.snu.cay.services.em.common.parameters.NumInitialEvals;
import edu.snu.cay.services.em.evaluator.api.BlockResolver;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * OperationRouter that redirects incoming operations on specific data ids to corresponding blocks and evaluators.
 * Note that this class is not thread-safe, which means client of this class must synchronize explicitly.
 * @param <K> type of data key
 */
@Private
@NotThreadSafe
public final class OperationRouter<K> {
  private static final Logger LOG = Logger.getLogger(OperationRouter.class.getName());

  private static final long INIT_WAIT_TIMEOUT_MS = 2000;
  private static final int MAX_NUM_INIT_REQUESTS = 3;

  /**
   * A volatile type of boolean for checking the initialization of router.
   */
  private volatile boolean initialized = false;

  /**
   * A boolean representing whether the evaluator is added by EM.add().
   */
  private final boolean addedEval;

  /**
   * A prefix of evaluator id will be set by {@link #initialize(String)} or {@link #initialize(List)} once,
   * and used by {@link #getEvalId(int)} to make the complete evaluator id.
   */
  private String evalPrefix;

  private final int localStoreId;

  private final BlockResolver<K> blockResolver;

  private final InjectionFuture<ElasticMemoryMsgSender> msgSender;

  /**
   * The number of total blocks.
   */
  private final int numTotalBlocks;

  /**
   * The number of initial Evaluators.
   */
  private final int numInitialEvals;

  /**
   * Array representing block locations.
   * Its index is the blockId and value is the storeId.
   */
  private final AtomicIntegerArray blockLocations;
  private final List<Integer> initialLocalBlocks;

  // TODO #380: we have to improve router to provide different routing tables for each dataType.
  @Inject
  private OperationRouter(final BlockResolver<K> blockResolver,
                          final InjectionFuture<ElasticMemoryMsgSender> msgSender,
                          @Parameter(NumTotalBlocks.class) final int numTotalBlocks,
                          @Parameter(NumInitialEvals.class) final int numInitialEvals,
                          @Parameter(MemoryStoreId.class) final int memoryStoreId,
                          @Parameter(AddedEval.class) final boolean addedEval) {
    this.blockResolver = blockResolver;
    this.msgSender = msgSender;
    this.localStoreId = memoryStoreId;
    this.numTotalBlocks = numTotalBlocks;
    this.numInitialEvals = numInitialEvals;
    this.addedEval = addedEval;

    final int numInitialLocalBlocks = addedEval ? 0 : (numTotalBlocks / numInitialEvals + 1); // +1 for remainders
    this.initialLocalBlocks = new ArrayList<>(numInitialLocalBlocks);
    this.blockLocations = new AtomicIntegerArray(numTotalBlocks);
  }

  /**
   * Initializes the router by providing a prefix of evaluator to locate remote evaluators.
   * For initial evaluators, it initializes the routing table by itself with a statically fixed scheme.
   * But, for evaluators added by EM.add, it sends a request to driver and
   * postpones the initialization until the response.
   * This method is invoked when the context is started.
   */
  public void initialize(final String endpointId) {
    // TODO #509: Remove assumption on the format of context id
    this.evalPrefix = endpointId.split("-")[0];
    LOG.log(Level.INFO, "Initialize router with localEndPointId: {0}", endpointId);

    if (!addedEval) {
      initRoutingTable();
      initialized = true;
    } else {
      requestRoutingTable();
    }
  }

  private void initRoutingTable() {
    // initial evaluators can initialize the routing table by itself
    for (int blockId = localStoreId; blockId < numTotalBlocks; blockId += numInitialEvals) {
      initialLocalBlocks.add(blockId);
    }

    // blocks are initially distributed across Evaluators in round-robin.
    for (int blockId = 0; blockId < numTotalBlocks; blockId++) {
      final int storeId = blockId % numInitialEvals;
      blockLocations.set(blockId, storeId);
    }
  }

  /**
   * Requests a routing table to driver.
   */
  private void requestRoutingTable() {
    LOG.log(Level.FINE, "Sends a request for the routing table");
    try (final TraceScope traceScope = Trace.startSpan("ROUTING_INIT_REQUEST")) {
      final TraceInfo traceInfo = TraceInfo.fromSpan(traceScope.getSpan());
      msgSender.get().sendRoutingTableInitReqMsg(traceInfo);
    }
  }

  /**
   * Initializes the routing table with the info received from the driver,
   * providing a prefix of evaluator to locate remote evaluators.
   * This method is for evaluators added by EM.add(), whose routing table should be updated dynamically.
   * It'd be invoked by the network response of {@link #requestRoutingTable()}.
   */
  public synchronized void initialize(final List<Integer> initBlockLocations) {
    if (initBlockLocations.size() != numTotalBlocks) {
      throw new RuntimeException("Imperfect routing table");
    }

    for (int blockId = 0; blockId < numTotalBlocks; blockId++) {
      final int storeId = initBlockLocations.get(blockId);

      // the evaluators initiated though this evaluators should not have any stores at the beginning
      if (storeId == localStoreId) {
        throw new RuntimeException("Wrong initial routing table");
      }
      this.blockLocations.set(blockId, storeId);
    }

    initialized = true;
    LOG.log(Level.FINE, "Operation router is initialized");
    synchronized (this) {
      // wake up all waiting threads
      this.notifyAll();
    }
  }

  /**
   * Checks the initialization of the routing table.
   * It returns if the routing table has been initialized,
   * otherwise requests initial routing table to driver and waits within a bounded time.
   * It throws RuntimeException, if the table is not initialized til the end.
   */
  private void checkInitialization() {
    if (initialized) {
      return;
    }

    // sends init request and waits for several times
    for (int reqCount = 0; reqCount < MAX_NUM_INIT_REQUESTS; reqCount++) {
      requestRoutingTable();

      LOG.log(Level.INFO, "Waiting {0} ms for router to be initialized", INIT_WAIT_TIMEOUT_MS);
      try {
        synchronized (this) {
          this.wait(INIT_WAIT_TIMEOUT_MS);
        }
      } catch (final InterruptedException e) {
        LOG.log(Level.WARNING, "Interrupted while waiting for router to be initialized", e);
      }

      if (initialized) {
        return;
      }
    }
    throw new RuntimeException("Fail to initialize the router");
  }

  /**
   * Routes the data key range of the operation. Note that this method must be synchronized to prevent other threads
   * from updating the routing information while reading it.
   * @param dataKeyRanges a range of data keys
   * @return a pair of a map between a block id and a corresponding sub key range,
   * and a map between evaluator id and corresponding sub key ranges.
   */
  public Pair<Map<Integer, List<Pair<K, K>>>, Map<String, List<Pair<K, K>>>> route(
      final List<Pair<K, K>> dataKeyRanges) {
    final Map<Integer, List<Pair<K, K>>> localBlockToSubKeyRangesMap = new HashMap<>();
    final Map<String, List<Pair<K, K>>> remoteEvalToSubKeyRangesMap = new HashMap<>();

    // dataKeyRanges has at least one element
    // In most cases, there are only one range in dataKeyRanges
    for (final Pair<K, K> keyRange : dataKeyRanges) {

      final Map<Integer, Pair<K, K>> blockToSubKeyRangeMap =
          blockResolver.resolveBlocksForOrderedKeys(keyRange.getFirst(), keyRange.getSecond());
      for (final Map.Entry<Integer, Pair<K, K>> blockToSubKeyRange : blockToSubKeyRangeMap.entrySet()) {
        final int blockId = blockToSubKeyRange.getKey();
        final Pair<K, K> minMaxKeyPair = blockToSubKeyRange.getValue();

        final Optional<String> remoteEvalId = resolveEval(blockId);

        // aggregate sub ranges
        if (remoteEvalId.isPresent()) {
          if (!remoteEvalToSubKeyRangesMap.containsKey(remoteEvalId.get())) {
            remoteEvalToSubKeyRangesMap.put(remoteEvalId.get(), new LinkedList<Pair<K, K>>());
          }
          final List<Pair<K, K>> remoteRangeList = remoteEvalToSubKeyRangesMap.get(remoteEvalId.get());
          remoteRangeList.add(minMaxKeyPair);
        } else {
          if (!localBlockToSubKeyRangesMap.containsKey(blockId)) {
            localBlockToSubKeyRangesMap.put(blockId, new LinkedList<Pair<K, K>>());
          }
          final List<Pair<K, K>> localRangeList = localBlockToSubKeyRangesMap.get(blockId);
          localRangeList.add(minMaxKeyPair);
        }
      }
    }

    return new Pair<>(localBlockToSubKeyRangesMap, remoteEvalToSubKeyRangesMap);
  }

  /**
   * Resolves an evaluator id for a block id.
   * It returns empty when the block belongs to the local MemoryStore.
   * Note that this method must be synchronized to prevent other threads
   * from updating the routing information while reading it.
   * @param blockId an id of block
   * @return an Optional with an evaluator id
   */
  public Optional<String> resolveEval(final int blockId) {
    checkInitialization();

    final int memoryStoreId = blockLocations.get(blockId);
    if (memoryStoreId == localStoreId) {
      return Optional.empty();
    } else {
      return Optional.of(getEvalId(memoryStoreId));
    }
  }

  /**
   * @return a list of block ids which are initially assigned to the local MemoryStore.
   */
  public List<Integer> getInitialLocalBlockIds() {
    checkInitialization();

    return Collections.unmodifiableList(initialLocalBlocks);
  }

  /**
   * @return a list of block ids which are currently assigned to the local MemoryStore.
   */
  public List<Integer> getCurrentLocalBlockIds() {
    checkInitialization();

    final List<Integer> localBlockIds = new ArrayList<>();
    for (int blockId = 0; blockId < blockLocations.length(); blockId++) {
      final int storeId = blockLocations.get(blockId);
      if (storeId == localStoreId) {
        localBlockIds.add(blockId);
      }
    }
    return localBlockIds;
  }

  /**
   * Updates the owner of the block. Note that this method must be synchronized
   * to prevent other threads from reading the routing information while updating it.
   * @param blockId id of the block to update its ownership.
   * @param oldOwnerId id of the MemoryStore that was owner.
   * @param newOwnerId id of the MemoryStore that will be new owner.
   */
  public void updateOwnership(final int blockId, final int oldOwnerId, final int newOwnerId) {
    checkInitialization();

    final int localOldOwnerId = blockLocations.getAndSet(blockId, newOwnerId);
    if (localOldOwnerId != oldOwnerId) {
      LOG.log(Level.WARNING, "Local routing table thought block {0} was in store {1}, but it was actually in {2}",
          new Object[]{blockId, oldOwnerId, newOwnerId});
    }
    LOG.log(Level.FINE, "Ownership of {0} is updated from {1} to {2}", new Object[]{blockId, oldOwnerId, newOwnerId});
  }

  /**
   * Converts the MemoryStore id to the corresponding Evaluator's endpoint id.
   * MemoryStore id is assumed to be assigned by the suffix of context id
   * (See {@link edu.snu.cay.services.em.driver.impl.PartitionManager#registerEvaluator(String, int)})
   * @param memoryStoreId MemoryStore's identifier
   * @return the endpoint id to access the MemoryStore.
   */
  private String getEvalId(final int memoryStoreId) {
    return evalPrefix + '-' + memoryStoreId;
  }
}
