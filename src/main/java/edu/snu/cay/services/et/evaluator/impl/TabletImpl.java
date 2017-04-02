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
import edu.snu.cay.services.et.evaluator.api.*;
import edu.snu.cay.services.et.exceptions.BlockNotExistsException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.annotations.audience.EvaluatorSide;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;

/**
 * An implementation of {@link Tablet}.
 */
@EvaluatorSide
@ThreadSafe
public final class TabletImpl<K, V, U> implements Tablet<K, V, U> {
  /**
   * Local blocks which this executor owns.
   */
  private final BlockStore<K, V> blockStore;

  /**
   * Local cache for ownership mapping.
   */
  private final OwnershipCache ownershipCache;

  /**
   * A function for updating values in the table.
   */
  private final UpdateFunction<K, V, U> updateFunction;

  @Inject
  private TabletImpl(final BlockStore<K, V> blockStore,
                     final OwnershipCache ownershipCache,
                     final UpdateFunction<K, V, U> updateFunction) {
    this.blockStore = blockStore;
    this.ownershipCache = ownershipCache;
    this.updateFunction = updateFunction;
  }

  @Override
  public V put(final int blockId, final K key, final V value) throws BlockNotExistsException {
    final Block<K, V> block = blockStore.get(blockId);
    return block.put(key, value);
  }

  @Override
  public V putIfAbsent(final int blockId, final K key, final V value) throws BlockNotExistsException {
    final Block<K, V> block = blockStore.get(blockId);
    return block.putIfAbsent(key, value);
  }

  @Override
  public V get(final int blockId, final K key) throws BlockNotExistsException {
    final Block<K, V> block = blockStore.get(blockId);
    return block.get(key);
  }

  @Override
  public V update(final int blockId, final K key, final U updateValue) throws BlockNotExistsException {
    final Block<K, V> block = blockStore.get(blockId);
    return block.update(key, updateValue, updateFunction);
  }

  @Override
  public V remove(final int blockId, final K key) throws BlockNotExistsException {
    final Block<K, V> block = blockStore.get(blockId);
    return block.remove(key);
  }

  @Override
  public Map<K, V> getDataMap() {
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
  public synchronized Iterator<Entry<K, V>> getDataIterator() {
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
}
