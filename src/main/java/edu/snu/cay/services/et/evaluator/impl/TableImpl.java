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
package edu.snu.cay.services.et.evaluator.impl;

import com.google.common.collect.Iterators;
import edu.snu.cay.services.et.avro.OpType;
import edu.snu.cay.services.et.configuration.parameters.TableIdentifier;
import edu.snu.cay.services.et.evaluator.api.*;
import edu.snu.cay.services.et.exceptions.BlockNotExistsException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;

/**
 * An implementation of {@link Table}.
 */
@EvaluatorSide
@ThreadSafe
public final class TableImpl<K, V, U> implements Table<K, V, U>, TableComponents<K, V, U> {
  /**
   * Table identifier.
   */
  private final String tableId;

  /**
   * Local cache for ownership mapping.
   */
  private final OwnershipCache ownershipCache;

  /**
   * Local blocks which this executor owns.
   */
  private final BlockStore<K, V> blockStore;

  /**
   * A serializer for both key and value of a table.
   */
  private final KVUSerializer<K, V, U> kvuSerializer;

  /**
   * A function for updating values in the table.
   */
  private final UpdateFunction<K, V, U> updateFunction;

  /**
   * A component for accessing remote blocks.
   */
  private final RemoteAccessOpSender remoteAccessOpSender;

  /**
   * Partition function that resolves key into block id.
   */
  private final BlockPartitioner<K> blockPartitioner;

  @Inject
  private TableImpl(@Parameter(TableIdentifier.class) final String tableId,
                    final OwnershipCache ownershipCache,
                    final BlockStore<K, V> blockStore,
                    final KVUSerializer<K, V, U> kvuSerializer,
                    final UpdateFunction<K, V, U> updateFunction,
                    final RemoteAccessOpSender remoteAccessOpSender,
                    final BlockPartitioner<K> blockPartitioner) {
    this.tableId = tableId;
    this.ownershipCache = ownershipCache;
    this.blockStore = blockStore;
    this.kvuSerializer = kvuSerializer;
    this.updateFunction = updateFunction;
    this.remoteAccessOpSender = remoteAccessOpSender;
    this.blockPartitioner = blockPartitioner;
  }

  @Override
  public V put(final K key, final V value) {

    final int blockId = blockPartitioner.getBlockId(key);
    final Optional<String> remoteIdOptional;

    final Pair<Optional<String>, Lock> remoteIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
    try {
      remoteIdOptional = remoteIdWithLock.getKey();

      // execute operation in local, holding ownershipLock
      if (!remoteIdOptional.isPresent()) {
        final Block<K, V> block = blockStore.get(blockId);
        return block.put(key, value);
      }
    } catch (final BlockNotExistsException e) {
      throw new RuntimeException(e);
    } finally {
      final Lock ownershipLock = remoteIdWithLock.getValue();
      ownershipLock.unlock();
    }

    // send operation to remote and wait until operation is finished
    final DataOpResult<V> opResult = remoteAccessOpSender.sendOpToRemote(
        OpType.PUT, tableId, blockId, key, value, null, remoteIdOptional.get(), true);

    return opResult.getOutputData();
  }

  @Override
  public DataOpResult<V> putAsync(final K key, final V value) {

    final int blockId = blockPartitioner.getBlockId(key);
    final Optional<String> remoteIdOptional;

    final Pair<Optional<String>, Lock> remoteIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
    try {
      remoteIdOptional = remoteIdWithLock.getKey();

      // execute operation in local, holding ownershipLock
      if (!remoteIdOptional.isPresent()) {
        final Block<K, V> block = blockStore.get(blockId);
        final V result = block.put(key, value);
        return new DataOpResultImpl<>(result, true);
      }
    } catch (final BlockNotExistsException e) {
      throw new RuntimeException(e);
    } finally {
      final Lock ownershipLock = remoteIdWithLock.getValue();
      ownershipLock.unlock();
    }

    // send operation to remote
    final DataOpResult<V> opResult = remoteAccessOpSender.sendOpToRemote(
        OpType.PUT, tableId, blockId, key, value, null, remoteIdOptional.get(), false);

    return opResult;
  }

  @Override
  public V get(final K key) {
    final int blockId = blockPartitioner.getBlockId(key);
    final Optional<String> remoteIdOptional;

    final Pair<Optional<String>, Lock> remoteIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
    try {
      remoteIdOptional = remoteIdWithLock.getKey();

      // execute operation in local, holding ownershipLock
      if (!remoteIdOptional.isPresent()) {
        final Block<K, V> block = blockStore.get(blockId);
        return block.get(key);
      }
    } catch (final BlockNotExistsException e) {
      throw new RuntimeException(e);
    } finally {
      final Lock ownershipLock = remoteIdWithLock.getValue();
      ownershipLock.unlock();
    }

    // send operation to remote and wait until operation is finished
    final DataOpResult<V> opResult = remoteAccessOpSender.sendOpToRemote(
        OpType.GET, tableId, blockId, key, null, null, remoteIdOptional.get(), true);

    return opResult.getOutputData();
  }

  @Override
  public DataOpResult<V> getAsync(final K key) {
    final int blockId = blockPartitioner.getBlockId(key);
    final Optional<String> remoteIdOptional;

    final Pair<Optional<String>, Lock> remoteIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
    try {
      remoteIdOptional = remoteIdWithLock.getKey();

      // execute operation in local, holding ownershipLock
      if (!remoteIdOptional.isPresent()) {
        final Block<K, V> block = blockStore.get(blockId);
        final V result = block.get(key);
        return new DataOpResultImpl<>(result, true);
      }
    } catch (final BlockNotExistsException e) {
      throw new RuntimeException(e);
    } finally {
      final Lock ownershipLock = remoteIdWithLock.getValue();
      ownershipLock.unlock();
    }

    // send operation to remote and wait until operation is finished
    final DataOpResult<V> opResult = remoteAccessOpSender.sendOpToRemote(
        OpType.GET, tableId, blockId, key, null, null, remoteIdOptional.get(), false);

    return opResult;
  }

  @Override
  public V update(final K key, final U updateValue) {
    final int blockId = blockPartitioner.getBlockId(key);
    final Optional<String> remoteIdOptional;

    final Pair<Optional<String>, Lock> remoteIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
    try {
      remoteIdOptional = remoteIdWithLock.getKey();

      // execute operation in local, holding ownershipLock
      if (!remoteIdOptional.isPresent()) {
        final Block<K, V> block = blockStore.get(blockId);
        return block.update(key, updateValue, updateFunction);
      }
    } catch (final BlockNotExistsException e) {
      throw new RuntimeException(e);
    } finally {
      final Lock ownershipLock = remoteIdWithLock.getValue();
      ownershipLock.unlock();
    }

    // send operation to remote and wait until operation is finished
    final DataOpResult<V> opResult = remoteAccessOpSender.sendOpToRemote(
        OpType.UPDATE, tableId, blockId, key, null, updateValue, remoteIdOptional.get(), true);

    return opResult.getOutputData();
  }

  @Override
  public DataOpResult<V> updateAsync(final K key, final U updateValue) {
    final int blockId = blockPartitioner.getBlockId(key);
    final Optional<String> remoteIdOptional;

    final Pair<Optional<String>, Lock> remoteIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
    try {
      remoteIdOptional = remoteIdWithLock.getKey();

      // execute operation in local, holding ownershipLock
      if (!remoteIdOptional.isPresent()) {
        final Block<K, V> block = blockStore.get(blockId);
        final V result = block.update(key, updateValue, updateFunction);
        return new DataOpResultImpl<>(result, true);
      }
    } catch (final BlockNotExistsException e) {
      throw new RuntimeException(e);
    } finally {
      final Lock ownershipLock = remoteIdWithLock.getValue();
      ownershipLock.unlock();
    }

    // send operation to remote and wait until operation is finished
    final DataOpResult<V> opResult = remoteAccessOpSender.sendOpToRemote(
        OpType.UPDATE, tableId, blockId, key, null, updateValue, remoteIdOptional.get(), false);

    return opResult;
  }

  @Override
  public V remove(final K key) {
    final int blockId = blockPartitioner.getBlockId(key);
    final Optional<String> remoteIdOptional;

    final Pair<Optional<String>, Lock> remoteIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
    try {
      remoteIdOptional = remoteIdWithLock.getKey();

      // execute operation in local, holding ownershipLock
      if (!remoteIdOptional.isPresent()) {
        final Block<K, V> block = blockStore.get(blockId);
        return block.remove(key);
      }
    } catch (final BlockNotExistsException e) {
      throw new RuntimeException(e);
    } finally {
      final Lock ownershipLock = remoteIdWithLock.getValue();
      ownershipLock.unlock();
    }

    // send operation to remote and wait until operation is finished
    final DataOpResult<V> opResult = remoteAccessOpSender.sendOpToRemote(
        OpType.REMOVE, tableId, blockId, key, null, null, remoteIdOptional.get(), true);

    return opResult.getOutputData();
  }

  @Override
  public DataOpResult<V> removeAsync(final K key) {
    final int blockId = blockPartitioner.getBlockId(key);
    final Optional<String> remoteIdOptional;

    final Pair<Optional<String>, Lock> remoteIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
    try {
      remoteIdOptional = remoteIdWithLock.getKey();

      // execute operation in local, holding ownershipLock
      if (!remoteIdOptional.isPresent()) {
        final Block<K, V> block = blockStore.get(blockId);
        final V result = block.remove(key);
        return new DataOpResultImpl<>(result, true);
      }
    } catch (final BlockNotExistsException e) {
      throw new RuntimeException(e);
    } finally {
      final Lock ownershipLock = remoteIdWithLock.getValue();
      ownershipLock.unlock();
    }

    // send operation to remote and wait until operation is finished
    final DataOpResult<V> opResult = remoteAccessOpSender.sendOpToRemote(
        OpType.REMOVE, tableId, blockId, key, null, null, remoteIdOptional.get(), false);

    return opResult;
  }

  @Override
  public Map<K, V> getLocalDataMap() {
    Map<K, V> result = null;

    // will not care blocks migrated in after this call
    final List<Integer> localBlockIds = ownershipCache.getCurrentLocalBlockIds();

    for (final Integer blockId : localBlockIds) {
      final Pair<Optional<String>, Lock> remoteEvalIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
      try {
        if (!remoteEvalIdWithLock.getKey().isPresent()) { // scan block if it still remains in local
          final Block<K, V> block = blockStore.get(blockId);

          if (result == null) {
            // reuse the first returned map object
            result = block.getAll();
          } else {
            // huge memory pressure may happen here
            result.putAll(block.getAll());
          }
        }
      } catch (final BlockNotExistsException e) {
        throw new RuntimeException(e);
      } finally {
        remoteEvalIdWithLock.getValue().unlock();
      }
    }

    return result == null ? Collections.emptyMap() : result;
  }


  /**
   * It utilizes iterator of {@link java.util.concurrent.ConcurrentHashMap}.
   * This iterator returns elements reflecting the concurrent modifications
   * without any synchronization, so it sees transient states of the map.
   * It means that you can miss updates since the iteration began.
   */
  @Override
  public synchronized Iterator<Entry<K, V>> getLocalDataIterator() {
    return new Iterator<Entry<K, V>>() {
      private final Iterator<Block<K, V>> blockIterator = blockStore.iterator();
      private Iterator<Entry<K, V>> entryIterator = Iterators.emptyIterator();

      private boolean checkNextBlocks() {
        while (!entryIterator.hasNext()) {
          if (!blockIterator.hasNext()) {
            return false;
          }
          entryIterator = blockIterator.next().iterator();
        }

        return true;
      }

      @Override
      public boolean hasNext() {
        return checkNextBlocks();
      }

      @Override
      public Entry<K, V> next() {
        checkNextBlocks();
        return entryIterator.next();
      }
    };
  }

  @Override
  public OwnershipCache getOwnershipCache() {
    return ownershipCache;
  }

  @Override
  public BlockStore<K, V> getBlockStore() {
    return blockStore;
  }

  @Override
  public UpdateFunction<K, V, U> getUpdateFunction() {
    return updateFunction;
  }

  @Override
  public KVUSerializer<K, V, U> getSerializer() {
    return kvuSerializer;
  }

  public BlockPartitioner<K> getBlockPartitioner() {
    return blockPartitioner;
  }
}
