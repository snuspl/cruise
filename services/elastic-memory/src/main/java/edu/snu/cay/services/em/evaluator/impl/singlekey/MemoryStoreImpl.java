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
import org.apache.commons.lang.NotImplementedException;
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
 * All data is stored in multiple Blocks embedding a {@code ConcurrentHashMap}.
 * Assuming EM applications always need to instantiate this class, HTrace initialization is done in the constructor.
 */
public final class MemoryStoreImpl<K> implements RemoteAccessibleMemoryStore<K> {
  private static final Logger LOG = Logger.getLogger(MemoryStore.class.getName());

  private static final int QUEUE_SIZE = 1024;
  private static final int QUEUE_TIMEOUT_MS = 3000;

  /**
   * Maintains blocks associated with blockIds.
   */
  private final ConcurrentMap<Integer, Block> blocks = new ConcurrentHashMap<>();

  @GuardedBy("routerLock")
  private final OperationRouter<K> router;
  private final BlockResolver<K> blockResolver;
  private final RemoteOpHandlerImpl<K> remoteOpHandlerImpl;

  /**
   * An update function to be used in {@link Block#update}.
   * We assume that there's only one function for the store.
   */
  private final EMUpdateFunction<K, ?> updateFunction;

  private final ReadWriteLock routerLock = new ReentrantReadWriteLock(true);

  /**
   * A queue for operations requested from remote clients.
   */
  private final BlockingQueue<SingleKeyOperation<K, Object>> operationQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);

  @Inject
  private MemoryStoreImpl(final HTrace hTrace,
                          final OperationRouter<K> router,
                          final BlockResolver<K> blockResolver,
                          final RemoteOpHandlerImpl<K> remoteOpHandlerImpl,
                          final EMUpdateFunction<K, ?> updateFunction,
                          @Parameter(NumStoreThreads.class) final int numStoreThreads) {
    hTrace.initialize();
    this.router = router;
    this.blockResolver = blockResolver;
    this.remoteOpHandlerImpl = remoteOpHandlerImpl;
    this.updateFunction = updateFunction;
    initBlocks();
    initExecutor(numStoreThreads);
  }

  /**
   * Initialize the blocks in the local MemoryStore.
   */
  private void initBlocks() {
    for (final int blockId : router.getInitialLocalBlockIds()) {
      blocks.put(blockId, new Block());
    }
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

  @Override
  public void updateOwnership(final int blockId, final int oldOwnerId, final int newOwnerId) {
    routerLock.writeLock().lock();
    try {
      router.updateOwnership(blockId, oldOwnerId, newOwnerId);
    } finally {
      routerLock.writeLock().unlock();
    }
  }

  @Override
  public void putBlock(final int blockId, final Map<K, Object> data) {
    final Block block = new Block();
    block.putAll(data);

    if (blocks.putIfAbsent(blockId, block) != null) {
      throw new RuntimeException("Block with id " + blockId + " already exists.");
    }
  }

  @Override
  public Map<K, Object> getBlock(final int blockId) {
    final Block block = blocks.get(blockId);
    if (null == block) {
      throw new RuntimeException("Block with id " + blockId + "does not exist.");
    }

    return block.getAll();
  }

  @Override
  public void removeBlock(final int blockId) {
    final Block block = blocks.remove(blockId);
    if (null == block) {
      throw new RuntimeException("Block with id " + blockId + "does not exist.");
    }
  }

  @Override
  public boolean registerBlockUpdateListener(final BlockUpdateListener listener) {
    throw new NotImplementedException();
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
          case UPDATE:
            output = block.update(operation.getKey(), operation.getValue().get());
            break;
          default:
            LOG.log(Level.WARNING, "Undefined type of operation.");
            output = null;
            isSuccess = false;
          }

          remoteOpHandlerImpl.sendResultToOrigin(operation, Optional.ofNullable(output), isSuccess);
        } else {
          LOG.log(Level.WARNING,
              "Failed to execute operation {0} requested by remote store {2}. This store was considered as the owner" +
                  " of block {1} by store {2}, but the local router assumes store {3} is the owner",
              new Object[]{operation.getOpId(), blockId, operation.getOrigEvalId().get(), remoteEvalId.get()});

          // send the failed result
          remoteOpHandlerImpl.sendResultToOrigin(operation, Optional.<V>empty(), false);
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
    private final EMUpdateFunction<K, V> emUpdateFunction = (EMUpdateFunction<K, V>) updateFunction;

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
     * Updates the value associated with the given {@code key} using {@code deltaValue}.
     */
    private V update(final K key, final V deltaValue) {
      return subDataMap.compute(key, (k, v) -> {
          final V oldValue = (v == null) ? emUpdateFunction.getInitValue(k) : v;
          return emUpdateFunction.getUpdateValue(oldValue, deltaValue);
        });
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
    LOG.log(Level.FINEST, "Enqueue Op. OpId: {0}", operation.getOpId());
    try {
      operationQueue.put(operation);
    } catch (final InterruptedException e) {
      LOG.log(Level.SEVERE, "Enqueue failed with InterruptedException", e);
    }
  }

  @Override
  public <V> Pair<K, Boolean> put(final K id, @Nonnull final V value) {

    final int blockId = blockResolver.resolveBlock(id);
    final Optional<String> remoteEvalId = router.resolveEval(blockId);

    // execute operation in local or send it to remote
    if (remoteEvalId.isPresent()) {
      // send operation to remote and wait until operation is finished
      final SingleKeyOperation<K, V> operation =
          remoteOpHandlerImpl.sendOpToRemoteStore(DataOpType.PUT, id, Optional.of(value), remoteEvalId.get());

      return new Pair<>(id, operation.isSuccess());
    } else {
      final Block<V> block = blocks.get(blockId);
      block.put(id, value);
      return new Pair<>(id, true);
    }
  }

  @Override
  public <V> Map<K, Boolean> putList(final List<K> ids, final List<V> values) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <V> Pair<K, V> get(final K id) {

    final int blockId = blockResolver.resolveBlock(id);
    final Optional<String> remoteEvalId = router.resolveEval(blockId);

    // execute operation in local or send it to remote
    if (remoteEvalId.isPresent()) {
      // send operation to remote and wait until operation is finished
      final SingleKeyOperation<K, V> operation =
          remoteOpHandlerImpl.sendOpToRemoteStore(DataOpType.GET, id, Optional.<V>empty(), remoteEvalId.get());

      final V outputData = operation.getOutputData().get();
      return outputData == null ? null : new Pair<>(id, outputData);
    } else {
      final Block<V> block = blocks.get(blockId);
      final V output = block.get(id);
      return output == null ? null : new Pair<>(id, output);
    }
  }

  @Override
  public <V> Map<K, V> getAll() {
    final Map<K, V> result;

    final Iterator<Block> blockIterator = blocks.values().iterator();

    // first execute on a head block to reuse the returned map object for a return map
    if (blockIterator.hasNext()) {
      final Block<V> block = blockIterator.next();
      result = block.getAll();
    } else {
      return Collections.emptyMap();
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
  public <V> Map<K, V> getRange(final K startId, final K endId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <V> Pair<K, V> update(final K id, final V deltaValue) {
    final int blockId = blockResolver.resolveBlock(id);
    final Optional<String> remoteEvalId = router.resolveEval(blockId);

    if (remoteEvalId.isPresent()) {
      final SingleKeyOperation<K, V> operation =
          remoteOpHandlerImpl.sendOpToRemoteStore(DataOpType.UPDATE, id, Optional.of(deltaValue), remoteEvalId.get());

      return new Pair<>(id, operation.getOutputData().get());
    } else {
      final Block<V> block = blocks.get(blockId);
      final V output = block.update(id, deltaValue);
      return new Pair<>(id, output);
    }
  }

  @Override
  public <V> Pair<K, V> remove(final K id) {

    final int blockId = blockResolver.resolveBlock(id);
    final Optional<String> remoteEvalId = router.resolveEval(blockId);

    // execute operation in local or send it to remote
    if (remoteEvalId.isPresent()) {
      // send operation to remote and wait until operation is finished
      final SingleKeyOperation<K, V> operation =
          remoteOpHandlerImpl.sendOpToRemoteStore(DataOpType.REMOVE, id, Optional.<V>empty(), remoteEvalId.get());

      final V outputData = operation.getOutputData().get();
      return outputData == null ? null : new Pair<>(id, outputData);
    } else {
      final Block<V> block = blocks.get(blockId);
      final V output = block.remove(id);
      return output == null ? null : new Pair<>(id, output);
    }
  }

  @Override
  public <V> Map<K, V> removeAll() {
    final Map<K, V> result;

    final Iterator<Block> blockIterator = blocks.values().iterator();

    // first execute on a head block to reuse the returned map object for a return map
    if (blockIterator.hasNext()) {
      final Block<V> block = blockIterator.next();
      result = block.removeAll();
    } else {
      return Collections.emptyMap();
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
  public <V> Map<K, V> removeRange(final K startId, final K endId) {
    throw new UnsupportedOperationException();
  }

  /**
   * @return the number of blocks of specific type
   */
  @Override
  public int getNumBlocks() {
    return blocks.size();
  }
}
