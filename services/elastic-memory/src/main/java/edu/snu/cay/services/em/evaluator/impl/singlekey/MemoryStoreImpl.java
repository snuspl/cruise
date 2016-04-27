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
package edu.snu.cay.services.em.evaluator.impl.singlekey;

import edu.snu.cay.services.em.avro.DataOpType;
import edu.snu.cay.services.em.common.parameters.NumStoreThreads;
import edu.snu.cay.services.em.evaluator.api.*;
import edu.snu.cay.services.em.evaluator.impl.OperationRouter;
import edu.snu.cay.utils.trace.HTrace;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class MemoryStoreImpl<K> implements RemoteAccessibleMemoryStore<K> {
  private static final Logger LOG = Logger.getLogger(MemoryStore.class.getName());

  private static final int QUEUE_SIZE = 1024;
  private static final int QUEUE_TIMEOUT_MS = 3000;


  private final Map<String, Map<Integer, Block>> typeToBlocks = new HashMap<>();

  private final OperationRouter<K> router;
  private final BlockResolver<K> blockResolver;

  private final RemoteOpHandler<K> remoteOpHandler;

  private final ReadWriteLock routerLock = new ReentrantReadWriteLock(true);

  private final AtomicLong operationIdCounter = new AtomicLong(0);

  private final BlockingQueue<SingleKeyOperation<K, Object>> operationQueue = new ArrayBlockingQueue(QUEUE_SIZE);

  @Inject
  private MemoryStoreImpl(final HTrace hTrace,
                          final OperationRouter<K> router,
                          final BlockResolver<K> blockResolver,
                          final RemoteOpHandler remoteOpHandler,
                          @Parameter(NumStoreThreads.class) final int numStoreThreads) {
    hTrace.initialize();
    this.router = router;
    this.blockResolver = blockResolver;
    this.remoteOpHandler = remoteOpHandler;
    initExecutor(numStoreThreads);
  }

  /**
   * Initialize threads that dequeue and execute operation from the {@code subOperationQueue}.
   * That is, these threads serve operations requested from remote clients.
   */
  private void initExecutor(final int numStoreThreads) {
    final ExecutorService executor = Executors.newFixedThreadPool(numStoreThreads);
    for (int i = 0; i < numStoreThreads; i++) {
      executor.submit(new OperationThread());
    }
  }

  /**
   * Initialize blocks for a specific {@code dataType}.
   * Each block holds the subset of the data that is assigned to this MemoryStore.
   */
  private synchronized void initBlocks(final String dataType) {
    if (typeToBlocks.containsKey(dataType)) {
      return;
    }

    final Map<Integer, Block> initialBlocks = new HashMap<>();
    // We don't need to lock router because this method is already synchronized.
    for (final int blockIdx : router.getInitialLocalBlockIds()) {
      initialBlocks.put(blockIdx, new Block());
    }

    // must put initialBlocks into typeToBlocks after completely initialize it
    typeToBlocks.put(dataType, initialBlocks);
  }

    /**
   * A runnable that dequeues and executes operations requested from remote clients.
   * Several threads are initiated at the beginning and run as long-running background services.
   */
  private final class OperationThread implements Runnable {

    @Override
    public void run() {
      while (true) {
        // First, poll and execute a single operation.
        // Poll with a timeout will prevent busy waiting, when the queue is empty.
        try {
          final SingleKeyOperation<K, Object> operation =
              operationQueue.poll(QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          if (operation == null) {
            continue;
          }

          handleOperation(operation);
        } catch (final InterruptedException e) {
          LOG.log(Level.SEVERE, "Poll failed with InterruptedException", e);
        }
      }
    }

    private <V> void handleOperation(final SingleKeyOperation<K, V> operation) {
      final int blockId = blockResolver.resolveBlock(operation.getKey());

      LOG.log(Level.FINEST, "Poll op: [OpId: {0}, origId: {1}, block: {2}]]",
          new Object[]{operation.getOpId(), operation.getOrigEvalId().get(), blockId});

      routerLock.readLock().lock();
      try {
        final Optional<String> remoteEvalId = router.resolveEval(blockId);
        final boolean isLocal = !remoteEvalId.isPresent();
        if (isLocal) {
          final String dataType = operation.getDataType();
          final Map<Integer, Block> blocks = typeToBlocks.get(dataType);
          final Block<V> block = blocks.get(blockId);
          final V result = block.executeOperation(operation);
          submitLocalResult(operation, Optional.ofNullable(result), true);
        } else {
          LOG.log(Level.WARNING,
              "This MemoryStore was considered the Block {0}'s owner, but the local router assumes {1} as the owner",
              new Object[]{blockId, remoteEvalId.get()});

          // submit it as a local result, because we do not even start the remote operation
          submitLocalResult(operation, Optional.<V>empty(), false);
        }
      } finally {
        routerLock.readLock().unlock();
      }
    }
  }

  private final class Block<V> {
    private final ConcurrentMap<K, V> subDataMap = new ConcurrentHashMap<>();

    private V executeOperation(final SingleKeyOperation<K, V> operation) {
      final DataOpType opType = operation.getOpType();

      final V output;

      switch (opType) {
      case PUT:
        subDataMap.put(operation.getKey(), operation.getValue().get());
        output = null;
        break;
      case GET:
        output = subDataMap.get(operation.getKey());
        break;
      case REMOVE:
        output = subDataMap.remove(operation.getKey());
        break;
      default:
        throw new RuntimeException("Undefined operation");
      }

      return output;
    }

    private Map<K, V> getAll() {
      return new HashMap<>(subDataMap);
    }

    private Map<K, V> removeAll() {
      final Map<K, V> output = new HashMap<>(subDataMap);
      subDataMap.clear();
      return output;
    }

    private int getNumUnits() {
      return subDataMap.size();
    }
  }

  /**
   * Executes an operation requested from a local client.
   */
  private <V> void executeOperation(final SingleKeyOperation<K, V> operation) {

    final K dataKey = operation.getKey();
    final int blockId = blockResolver.resolveBlock(dataKey);
    final Optional<String> remoteEvalId = router.resolveEval(blockId);

    if (remoteEvalId.isPresent()) {
      // send operation to remote
      remoteOpHandler.sendOpToRemoteStore(operation, remoteEvalId.get());
    } else {
      // execute local
      final String dataType = operation.getDataType();

      // if there's no initialized block for a data type of the operation,
      // initialize blocks and continue the operation only when it's operation type is PUT.
      // for other types of operation do not execute the operation.
      if (!typeToBlocks.containsKey(dataType)) {
        if (operation.getOpType() == DataOpType.PUT) {
          initBlocks(dataType);
        } else {
          operation.commitResult(Optional.<V>empty(), false);
          return;
        }
      }

      final Map<Integer, Block> blocks = typeToBlocks.get(dataType);
      final Block<V> block = blocks.get(blockId);
      final V localOutput = block.executeOperation(operation);
      operation.commitResult(Optional.ofNullable(localOutput), true);
    }
  }

  /**
   * Handles the result of data operation processed by local memory store.
   * It waits until all remote sub operations are finished and their outputs are fully aggregated.
   */
  <V> void submitLocalResult(final SingleKeyOperation<K, V> operation, final Optional<V> localOutput,
                             final boolean isSuccess) {
    operation.commitResult(localOutput, isSuccess);

    LOG.log(Level.FINEST, "Local operation succeed. OpId: {0}", operation.getOpId());

    if (!operation.isFromLocalClient()) {
      remoteOpHandler.sendResultToOrigin(operation);
    }
  }

  @Override
  public <V> Pair<K, Boolean> put(final String dataType, final K id, @Nonnull final V value) {
    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final SingleKeyOperation<K, V> operation = new SingleKeyOperationImpl(Optional.<String>empty(), operationId,
        DataOpType.PUT, dataType, id, Optional.of(value));

    executeOperation(operation);

    return new Pair<>(id, operation.isSuccess());
  }

  @Override
  public <V> Map<K, Boolean> putList(final String dataType, final List<K> ids, final List<V> values) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <V> Pair<K, V> get(final String dataType, final K id) {
    final String operationId = Long.toString(operationIdCounter.getAndIncrement());

    final SingleKeyOperation<K, V> operation = new SingleKeyOperationImpl<>(Optional.<String>empty(), operationId,
        DataOpType.GET, dataType, id, Optional.<V>empty());

    executeOperation(operation);

    final V outputData = operation.getOutputData().get();

    return outputData == null ? null : new Pair<>(id, outputData);
  }

  @Override
  public <V> Map<K, V> getAll(final String dataType) {
    if (!typeToBlocks.containsKey(dataType)) {
      return Collections.EMPTY_MAP;
    }

    final Map<K, V> result;
    final Collection<Block> blocks = typeToBlocks.get(dataType).values();

    final Iterator<Block> blockIterator = blocks.iterator();

    // first execute on a head block to reuse the returned map object for a return map
    if (blockIterator.hasNext()) {
      final Block<V> block = blockIterator.next();
      result = block.getAll();
    } else {
      return Collections.EMPTY_MAP;
    }

    // execute on remaining blocks if exist
    while (blockIterator.hasNext()) {
      final Block<V> block = blockIterator.next();
      // huge memory pressure may happen here
      result.putAll(block.getAll());
    }

    return result;
  }

  @Override
  public <V> Map<K, V> getRange(final String dataType, final K startId, final K endId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <V> Pair<K, V> remove(final String dataType, final K id) {
    final String operationId = Long.toString(operationIdCounter.getAndIncrement());
    final SingleKeyOperation<K, V> operation = new SingleKeyOperationImpl<>(Optional.<String>empty(), operationId,
        DataOpType.REMOVE, dataType, id, Optional.<V>empty());

    executeOperation(operation);

    final V outputData = operation.getOutputData().get();

    return outputData == null ? null : new Pair<>(id, outputData);
  }

  @Override
  public <V> Map<K, V> removeAll(final String dataType) {
    if (!typeToBlocks.containsKey(dataType)) {
      return Collections.EMPTY_MAP;
    }

    final Map<K, V> result;
    final Collection<Block> blocks = typeToBlocks.get(dataType).values();


    final Iterator<Block> blockIterator = blocks.iterator();

    // first execute on a head block to reuse the returned map object for a return map
    if (blockIterator.hasNext()) {
      final Block<V> block = blockIterator.next();
      result = block.removeAll();
    } else {
      return Collections.EMPTY_MAP;
    }

    // execute on remaining blocks if exist
    while (blockIterator.hasNext()) {
      final Block<V> block = blockIterator.next();
      // huge memory pressure may happen here
      result.putAll(block.removeAll());
    }

    return result;
  }

  @Override
  public <V> Map<K, V> removeRange(final String dataType, final K startId, final K endId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getDataTypes() {
    return new HashSet<>(typeToBlocks.keySet());
  }

  @Override
  public int getNumUnits(final String dataType) {
    if (!typeToBlocks.containsKey(dataType)) {
      return 0;
    }

    int numUnits = 0;
    final Collection<Block> blocks = typeToBlocks.get(dataType).values();
    for (final Block block : blocks) {
      numUnits += block.getNumUnits();
    }
    return numUnits;
  }

  @Override
  public int updateOwnership(final String dataType, final int blockId, final int storeId) {
    routerLock.writeLock().lock();
    try {
      final int oldOwnerId = router.updateOwnership(blockId, storeId);
      return oldOwnerId;
    } finally {
      routerLock.writeLock().unlock();
    }
  }

  @Override
  public void putBlock(final String dataType, final int blockId, final Map<K, Object> data) {
    final Map<Integer, Block> blocks = typeToBlocks.get(dataType);
    if (null == blocks) {
      // If the blocks of the type have not been initialized, then create the blocks.
      initBlocks(dataType);
    } else if (blocks.containsKey(blockId)) {
      throw new RuntimeException("Block with id " + blockId + " already exists.");
    } else {
      final Block block = new Block();
      block.subDataMap.putAll(data);
      blocks.put(blockId, block);
    }
  }

  @Override
  public Map<K, Object> getBlock(final String dataType, final int blockId) {
    final Map<Integer, Block> blocks = typeToBlocks.get(dataType);
    if (null == blocks) {
      throw new RuntimeException("Data type " + dataType + " does not exist.");
    }

    final Block block = blocks.get(blockId);
    if (null == block) {
      throw new RuntimeException("Block with id " + blockId + " does not exist.");
    }

    return block.getAll();
  }

  @Override
  public void removeBlock(final String dataType, final int blockId) {
    final Map<Integer, Block> blocks = typeToBlocks.get(dataType);
    if (null == blocks) {
      throw new RuntimeException("Data type " + dataType + " does not exist.");
    }

    final Block block = blocks.remove(blockId);
    if (null == block) {
      throw new RuntimeException("Block with id " + blockId + " does not exist.");
    }
  }

  @Override
  public void onNext(final DataOperation dataOperation) {
    final SingleKeyOperation<K, Object> operation = (SingleKeyOperationImpl<K, Object>) dataOperation;
    final String dataType = operation.getDataType();

    // progress when there're blocks for dataType
    if (!typeToBlocks.containsKey(dataType)) {
      // nevertheless, for PUT operations initialize blocks and continue the operation
      if (operation.getOpType() == DataOpType.PUT) {
        initBlocks(dataType);
      } else {
        // submit empty result for other types of operations
        submitLocalResult(operation, Optional.empty(), false);

        LOG.log(Level.FINEST, "Blocks for the {0}. Send empty result for operation {1} from {2}",
            new Object[]{operation.getDataType(), operation.getOpId(), operation.getOrigEvalId().get()});
        return;
      }
    }

    LOG.log(Level.FINEST, "Enqueue Op. OpId: {0}", operation.getOpId());
    try {
      operationQueue.put(operation);
    } catch (final InterruptedException e) {
      LOG.log(Level.SEVERE, "Enqueue failed with InterruptedException", e);
    }
  }
}
