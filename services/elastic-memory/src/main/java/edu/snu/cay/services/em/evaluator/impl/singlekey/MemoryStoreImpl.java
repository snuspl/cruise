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
import edu.snu.cay.services.em.evaluator.impl.BlockStore;
import edu.snu.cay.services.em.evaluator.impl.OwnershipCache;
import edu.snu.cay.utils.trace.HTrace;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@code MemoryStore} implementation for a key of generic type, non-supporting range operations.
 * It routes operations to local {@link BlockStore} or remote through {@link RemoteOpHandler}
 * based on the routing result from {@link OwnershipCache}.
 * Assuming EM applications always need to instantiate this class, HTrace initialization is done in the constructor.
 */
public final class MemoryStoreImpl<K> implements RemoteAccessibleMemoryStore<K> {
  private static final Logger LOG = Logger.getLogger(MemoryStore.class.getName());

  private static final int QUEUE_SIZE = 1024;
  private static final int QUEUE_TIMEOUT_MS = 3000;

  private final OwnershipCache ownershipCache;
  private final BlockResolver<K> blockResolver;
  private final RemoteOpHandlerImpl<K> remoteOpHandlerImpl;
  private final BlockStore blockStore;

  /**
   * A queue for operations requested from remote clients.
   */
  private final BlockingQueue<SingleKeyOperation<K, Object>> operationQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);

  @Inject
  private MemoryStoreImpl(final HTrace hTrace,
                          final OwnershipCache ownershipCache,
                          final BlockResolver<K> blockResolver,
                          final RemoteOpHandlerImpl<K> remoteOpHandlerImpl,
                          final BlockStore blockStore,
                          @Parameter(NumStoreThreads.class) final int numStoreThreads) {
    hTrace.initialize();
    this.ownershipCache = ownershipCache;
    this.blockResolver = blockResolver;
    this.remoteOpHandlerImpl = remoteOpHandlerImpl;
    this.blockStore = blockStore;
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

  @Override
  public boolean registerBlockUpdateListener(final BlockUpdateListener listener) {
    return blockStore.registerBlockUpdateListener(listener);
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

      final Tuple<Optional<String>, Lock> remoteEvalIdWithLock = ownershipCache.resolveEvalWithLock(blockId);
      try {
        final Optional<String> remoteEvalIdOptional = remoteEvalIdWithLock.getKey();
        final boolean isLocal = !remoteEvalIdOptional.isPresent();
        if (isLocal) {
          final BlockImpl<K, V> block = (BlockImpl<K, V>) blockStore.get(blockId);

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
                  " of block {1} by store {2}, but the local ownershipCache assumes store {3} is the owner",
              new Object[]{operation.getOpId(), blockId, operation.getOrigEvalId().get(), remoteEvalIdOptional.get()});

          // send the failed result
          remoteOpHandlerImpl.sendResultToOrigin(operation, Optional.<V>empty(), false);
        }
      } finally {
        final Lock ownershipLock = remoteEvalIdWithLock.getValue();
        ownershipLock.unlock();
      }
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
    final Optional<String> remoteEvalIdOptional;

    final Tuple<Optional<String>, Lock> remoteEvalIdWithLock = ownershipCache.resolveEvalWithLock(blockId);
    try {
      remoteEvalIdOptional = remoteEvalIdWithLock.getKey();

      // execute operation in local, holding ownershipLock
      if (!remoteEvalIdOptional.isPresent()) {
        final BlockImpl<K, V> block = (BlockImpl<K, V>) blockStore.get(blockId);
        block.put(id, value);
        return new Pair<>(id, true);
      }
    } finally {
      final Lock ownershipLock = remoteEvalIdWithLock.getValue();
      ownershipLock.unlock();
    }

    // send operation to remote and wait until operation is finished
    final SingleKeyOperation<K, V> operation =
        remoteOpHandlerImpl.sendOpToRemoteStore(DataOpType.PUT, id, Optional.of(value), remoteEvalIdOptional.get());

    return new Pair<>(id, operation.isSuccess());
  }

  @Override
  public <V> Map<K, Boolean> putList(final List<K> ids, final List<V> values) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <V> Pair<K, V> get(final K id) {
    final int blockId = blockResolver.resolveBlock(id);
    final Optional<String> remoteEvalIdOptional;

    final Tuple<Optional<String>, Lock> remoteEvalIdWithLock = ownershipCache.resolveEvalWithLock(blockId);
    try {
      remoteEvalIdOptional = remoteEvalIdWithLock.getKey();

      // execute operation in local, holding ownershipLock
      if (!remoteEvalIdOptional.isPresent()) {
        final BlockImpl<K, V> block = (BlockImpl<K, V>) blockStore.get(blockId);
        final V output = block.get(id);
        return output == null ? null : new Pair<>(id, output);
      }
    } finally {
      final Lock ownershipLock = remoteEvalIdWithLock.getValue();
      ownershipLock.unlock();
    }

    // send operation to remote and wait until operation is finished
    final SingleKeyOperation<K, V> operation =
        remoteOpHandlerImpl.sendOpToRemoteStore(DataOpType.GET, id, Optional.<V>empty(), remoteEvalIdOptional.get());

    final V outputData = operation.getOutputData().get();
    return outputData == null ? null : new Pair<>(id, outputData);
  }

  @Override
  public <V> Map<K, V> getAll() {
    final Map<K, V> result;

    final List<Integer> localBlockIds = ownershipCache.getCurrentLocalBlockIds();
    final Iterator<Integer> blockIdIterator = localBlockIds.iterator();

    // first execute on a head block to reuse the returned map object for a return map
    if (blockIdIterator.hasNext()) {
      final Block<K, V> block = blockStore.get(blockIdIterator.next());
      result = block.getAll();
    } else {
      return Collections.emptyMap();
    }

    // execute on remaining blocks if exist
    while (blockIdIterator.hasNext()) {
      final Block<K, V> block = blockStore.get(blockIdIterator.next());
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
    final Optional<String> remoteEvalIdOptional;

    final Tuple<Optional<String>, Lock> remoteEvalIdWithLock = ownershipCache.resolveEvalWithLock(blockId);
    try {
      remoteEvalIdOptional = remoteEvalIdWithLock.getKey();

      // execute operation in local, holding ownershipLock
      if (!remoteEvalIdOptional.isPresent()) {
        final BlockImpl<K, V> block = (BlockImpl<K, V>) blockStore.get(blockId);
        final V output = block.update(id, deltaValue);
        return new Pair<>(id, output);
      }
    } finally {
      final Lock ownershipLock = remoteEvalIdWithLock.getValue();
      ownershipLock.unlock();
    }

    // send operation to remote and wait until operation is finished
    final SingleKeyOperation<K, V> operation =
        remoteOpHandlerImpl.sendOpToRemoteStore(DataOpType.UPDATE, id, Optional.of(deltaValue),
            remoteEvalIdOptional.get());

    return new Pair<>(id, operation.getOutputData().get());
  }

  @Override
  public <V> Pair<K, V> remove(final K id) {

    final int blockId = blockResolver.resolveBlock(id);
    final Optional<String> remoteEvalIdOptional;

    final Tuple<Optional<String>, Lock> remoteEvalIdWithLock = ownershipCache.resolveEvalWithLock(blockId);
    try {
      remoteEvalIdOptional = remoteEvalIdWithLock.getKey();

      // execute operation in local, holding ownershipLock
      if (!remoteEvalIdOptional.isPresent()) {
        final BlockImpl<K, V> block = (BlockImpl<K, V>) blockStore.get(blockId);
        final V output = block.remove(id);
        return output == null ? null : new Pair<>(id, output);
      }
    } finally {
      final Lock ownershipLock = remoteEvalIdWithLock.getValue();
      ownershipLock.unlock();
    }

    // send operation to remote and wait until operation is finished
    final SingleKeyOperation<K, V> operation =
        remoteOpHandlerImpl.sendOpToRemoteStore(DataOpType.REMOVE, id, Optional.<V>empty(), remoteEvalIdOptional.get());

    final V outputData = operation.getOutputData().get();
    return outputData == null ? null : new Pair<>(id, outputData);
  }

  @Override
  public <V> Map<K, V> removeAll() {
    final Map<K, V> result;

    final List<Integer> localBlockIds = ownershipCache.getCurrentLocalBlockIds();
    final Iterator<Integer> blockIdIterator = localBlockIds.iterator();

    // first execute on a head block to reuse the returned map object for a return map
    if (blockIdIterator.hasNext()) {
      final Block<K, V> block = blockStore.get(blockIdIterator.next());
      result = block.removeAll();
    } else {
      return Collections.emptyMap();
    }

    // execute on remaining blocks if exist
    while (blockIdIterator.hasNext()) {
      final Block<K, V> block = blockStore.get(blockIdIterator.next());
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
    return blockStore.getNumBlocks();
  }

  @Override
  public Optional<String> resolveEval(final K key) {
    final int blockId = blockResolver.resolveBlock(key);
    return ownershipCache.resolveEval(blockId);
  }
}
