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
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@code MemoryStore} implementation for a key of generic type, non-supporting range operations.
 * All data of one data type is stored in multiple Blocks embedding a {@code ConcurrentHashMap}.
 * These Blocks are then maintained as values of one big {@code HashMap}, which uses the data types as keys.
 * Assuming EM applications always need to instantiate this class, HTrace initialization is done in the constructor.
 */
public final class MemoryStoreImpl<K> implements RemoteAccessibleMemoryStore<K> {
  private static final Logger LOG = Logger.getLogger(MemoryStore.class.getName());

  private static final int QUEUE_SIZE = 1024;
  private static final int QUEUE_TIMEOUT_MS = 3000;

  /**
   * This map uses data types, represented as strings, for keys and inner map for values.
   * Each inner map serves as a collection of data of the same data type, which is composed of multiple Blocks.
   * Each inner map maintains mapping between a Block id and Block itself.
   */
  private final Map<String, Map<Integer, Block>> typeToBlocks = new HashMap<>();

  @GuardedBy("routerLock")
  private final OperationRouter<K> router;
  private final BlockResolver<K> blockResolver;
  private final RemoteOpHandler<K> remoteOpHandler;

  private final ReadWriteLock routerLock = new ReentrantReadWriteLock(true);

  /**
   * A queue for operations requested from remote clients.
   */
  private final BlockingQueue<SingleKeyOperation<K, Object>> operationQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);

  @Inject
  private MemoryStoreImpl(final HTrace hTrace,
                          final OperationRouter<K> router,
                          final BlockResolver<K> blockResolver,
                          final RemoteOpHandler<K> remoteOpHandler,
                          @Parameter(NumStoreThreads.class) final int numStoreThreads) {
    hTrace.initialize();
    this.router = router;
    this.blockResolver = blockResolver;
    this.remoteOpHandler = remoteOpHandler;
    initExecutor(numStoreThreads);
  }

  /**
   * Initialize threads that dequeue and execute operation from the {@code operationQueue}.
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
    for (final int blockId : router.getInitialLocalBlockIds()) {
      initialBlocks.put(blockId, new Block());
    }

    // must put initialBlocks into typeToBlocks after completely initialize it
    typeToBlocks.put(dataType, initialBlocks);
  }

  @Override
  public void updateOwnership(final String dataType, final int blockId, final int oldOwnerId, final int newOwnerId) {
    routerLock.writeLock().lock();
    try {
      router.updateOwnership(blockId, oldOwnerId, newOwnerId);
    } finally {
      routerLock.writeLock().unlock();
    }
  }

  @Override
  public void putBlock(final String dataType, final int blockId, final Map<K, Object> data) {
    final Map<Integer, Block> blocks = typeToBlocks.get(dataType);
    if (null == blocks) {
      // If this MemoryStore has never stored the data in this type.
      // Then just create a block with the received data, and put it in the Map associating with the data type.
      final Block block = new Block();
      block.putAll(data);
      final Map<Integer, Block> newBlocks = new HashMap<>();
      newBlocks.put(blockId, block);

      typeToBlocks.put(dataType, newBlocks);
      LOG.log(Level.INFO, "The block {0} in type {1} has moved in.", new Object[]{blockId, dataType});
    } else if (!blocks.containsKey(blockId)) {
      // If the MemoryStore has the data in this type, but the block is not the exact same with the received one.
      // Then just put the block.
      final Block block = new Block();
      block.putAll(data);
      blocks.put(blockId, block);
    } else {
      throw new RuntimeException("Block " + blockId + " already exists.");
    }
  }

  @Override
  public Map<K, Object> getBlock(final String dataType, final int blockId) {
    final Map<Integer, Block> blocks = typeToBlocks.get(dataType);
    if (null == blocks) {
      LOG.log(Level.WARNING, "Data in type {0} has never been stored. The result is empty", dataType);
      return Collections.emptyMap();
    }

    final Block block = typeToBlocks.get(dataType).get(blockId);
    if (null == block) {
      LOG.log(Level.WARNING, "Block with id {0} does not exist.", blockId);
      return Collections.emptyMap();
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

          final V output;
          boolean isSuccess = true;
          final DataOpType opType = operation.getOpType();
          switch (opType) {
          case PUT:
            block.put(operation.getKey(), operation.getValue().get());
            output = null;
            break;
          case GET:
            output = block.get(operation.getKey());
            break;
          case REMOVE:
            output = block.remove(operation.getKey());
            break;
          default:
            LOG.log(Level.WARNING, "Undefined type of operation.");
            output = null;
            isSuccess = false;
          }

          remoteOpHandler.sendResultToOrigin(operation, Optional.ofNullable(output), isSuccess);
        } else {
          LOG.log(Level.WARNING,
              "Failed to execute operation {0} requested by remote store {2}. This store was considered as the owner" +
                  " of block {1} by store {2}, but the local router assumes store {3} is the owner",
              new Object[]{operation.getOpId(), blockId, operation.getOrigEvalId().get(), remoteEvalId.get()});

          // send the failed result
          remoteOpHandler.sendResultToOrigin(operation, Optional.<V>empty(), false);
        }
      } finally {
        routerLock.readLock().unlock();
      }
    }
  }

  /**
   * Block class that has a {@code subDataMap}, which is an unit of EM's move.
   */
  private final class Block<V> {
    /**
     * The map serves as a collection of data in a Block.
     * Its implementation is {@code ConcurrentHashMap} to
     * maximize the performance of concurrent single-key operations.
     */
    private final ConcurrentMap<K, V> subDataMap = new ConcurrentHashMap<>();

    private void put(final K key, final V value) {
      subDataMap.put(key, value);
    }

    private V get(final K key) {
      return subDataMap.get(key);
    }

    private V remove(final K key) {
      return subDataMap.remove(key);
    }

    /**
     * Returns all data in a block.
     * It is for supporting getAll method of MemoryStore.
     */
    private Map<K, V> getAll() {
      return new HashMap<>(subDataMap);
    }

    /**
     * Puts all data from the given Map to the block.
     */
    private void putAll(final Map<K, V> toPut) {
      subDataMap.putAll(toPut);
    }

    /**
     * Removes all data in a block.
     * It is for supporting removeAll method of MemoryStore.
     */
    private Map<K, V> removeAll() {
      final Map<K, V> output = new HashMap<>(subDataMap);
      subDataMap.clear();
      return output;
    }

    /**
     * Returns the number of data in a block.
     * It is for supporting getNumUnits method of MemoryStore.
     */
    private int getNumUnits() {
      return subDataMap.size();
    }
  }

  /**
   * Handles operations requested from a remote client.
   */
  @Override
  public void onNext(final DataOperation dataOperation) {
    final SingleKeyOperation<K, Object> operation = (SingleKeyOperation<K, Object>) dataOperation;
    final String dataType = operation.getDataType();

    // progress when there're blocks for dataType
    if (!typeToBlocks.containsKey(dataType)) {
      // nevertheless, for PUT operations initialize blocks and continue the operation
      if (operation.getOpType() == DataOpType.PUT) {
        initBlocks(dataType);
      } else {
        // send empty result for other types of operations
        remoteOpHandler.sendResultToOrigin(operation, Optional.empty(), false);

        LOG.log(Level.FINE, "Blocks for the type {0} do not exist. Send empty result for operation {1} from {2}",
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

  @Override
  public <V> Pair<K, Boolean> put(final String dataType, final K id, @Nonnull final V value) {

    final int blockId = blockResolver.resolveBlock(id);
    final Optional<String> remoteEvalId = router.resolveEval(blockId);

    // execute operation in local or send it to remote
    if (remoteEvalId.isPresent()) {
      // send operation to remote and wait until operation is finished
      final SingleKeyOperation<K, V> operation =
          remoteOpHandler.sendOpToRemoteStore(DataOpType.PUT, dataType, id, Optional.of(value), remoteEvalId.get());

      return new Pair<>(id, operation.isSuccess());
    } else {
      // initialize blocks and continue the operation,
      // if there's no initialized block for a data type of the operation.
      final Map<Integer, Block> blocks;
      final Map<Integer, Block> blockMap = typeToBlocks.get(dataType);
      if (blockMap == null) {
        initBlocks(dataType);
        blocks = typeToBlocks.get(dataType);
      } else {
        blocks = blockMap;
      }

      final Block<V> block = blocks.get(blockId);
      block.put(id, value);
      return new Pair<>(id, true);
    }
  }

  @Override
  public <V> Map<K, Boolean> putList(final String dataType, final List<K> ids, final List<V> values) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <V> Pair<K, V> get(final String dataType, final K id) {

    final int blockId = blockResolver.resolveBlock(id);
    final Optional<String> remoteEvalId = router.resolveEval(blockId);

    // execute operation in local or send it to remote
    if (remoteEvalId.isPresent()) {
      // send operation to remote and wait until operation is finished
      final SingleKeyOperation<K, V> operation =
          remoteOpHandler.sendOpToRemoteStore(DataOpType.GET, dataType, id, Optional.<V>empty(), remoteEvalId.get());

      final V outputData = operation.getOutputData().get();
      return outputData == null ? null : new Pair<>(id, outputData);
    } else {
      // return if there's no initialized block for a data type of the operation
      final Map<Integer, Block> blockMap = typeToBlocks.get(dataType);
      if (blockMap == null) {
        LOG.log(Level.FINE, "Blocks for the type {0} do not exist", dataType);
        return null;
      }

      final Block<V> block = blockMap.get(blockId);
      final V output = block.get(id);
      return output == null ? null : new Pair<>(id, output);
    }
  }

  @Override
  public <V> Map<K, V> getAll(final String dataType) {
    final Map<Integer, Block> blockMap = typeToBlocks.get(dataType);
    if (blockMap == null) {
      LOG.log(Level.FINE, "Blocks for the type {0} do not exist", dataType);
      return Collections.EMPTY_MAP;
    }

    final Map<K, V> result;

    final Iterator<Block> blockIterator = blockMap.values().iterator();

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

    final int blockId = blockResolver.resolveBlock(id);
    final Optional<String> remoteEvalId = router.resolveEval(blockId);

    // execute operation in local or send it to remote
    if (remoteEvalId.isPresent()) {
      // send operation to remote and wait until operation is finished
      final SingleKeyOperation<K, V> operation =
          remoteOpHandler.sendOpToRemoteStore(DataOpType.REMOVE, dataType, id, Optional.<V>empty(), remoteEvalId.get());

      final V outputData = operation.getOutputData().get();
      return outputData == null ? null : new Pair<>(id, outputData);
    } else {
      // return if there's no initialized block for a data type of the operation
      final Map<Integer, Block> blockMap = typeToBlocks.get(dataType);
      if (blockMap == null) {
        LOG.log(Level.FINE, "Blocks for the type {0} do not exist", dataType);
        return null;
      }

      final Block<V> block = blockMap.get(blockId);
      final V output = block.remove(id);
      return output == null ? null : new Pair<>(id, output);
    }
  }

  @Override
  public <V> Map<K, V> removeAll(final String dataType) {
    final Map<Integer, Block> blockMap = typeToBlocks.get(dataType);
    if (blockMap == null) {
      LOG.log(Level.FINE, "Blocks for the type {0} do not exist", dataType);
      return Collections.EMPTY_MAP;
    }

    final Map<K, V> result;

    final Iterator<Block> blockIterator = blockMap.values().iterator();

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
    final Map<Integer, Block> blockMap = typeToBlocks.get(dataType);
    if (blockMap == null) {
      LOG.log(Level.FINE, "Blocks for the type {0} do not exist", dataType);
      return 0;
    }

    int numUnits = 0;
    for (final Block block : blockMap.values()) {
      numUnits += block.getNumUnits();
    }
    return numUnits;
  }

  /**
   * Returns the number of local blocks whose type is {@code dataType}.
   * @param dataType a type of data
   * @return the number of blocks of specific type
   */
  public int getNumBlocks(final String dataType) {
    final Map<Integer, Block> blocks = typeToBlocks.get(dataType);
    if (blocks == null) {
      LOG.log(Level.FINE, "Blocks for the type {0} do not exist", dataType);
      return 0;
    } else {
      return blocks.size();
    }
  }
}
