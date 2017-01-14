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
package edu.snu.cay.services.em.evaluator.impl.rangekey;

import edu.snu.cay.services.em.avro.DataOpType;
import edu.snu.cay.services.em.evaluator.api.Block;
import edu.snu.cay.services.em.evaluator.api.RangeKeyOperation;
import org.apache.reef.io.network.util.Pair;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Block class that has a {@code subDataMap}, which is an unit of EM's move.
 * Also it's a concurrency unit for data operations because it has a {@code ReadWriteLock},
 * which regulates accesses to {@code subDataMap}.
 * Data is stored in a {@code TreeMap}, ordered by data ids.
 */
final class BlockImpl<Long, V> implements Block<Long, V> {

  BlockImpl() {
  }

  /**
   * The map serves as a collection of data in a Block.
   * Its implementation {@code TreeMap} is used for guaranteeing log(n) read and write operations, especially
   * {@code getRange()} and {@code removeRange()} which are ranged queries based on the ids.
   */
  private final NavigableMap<Long, V> subDataMap = new TreeMap<>();

  /**
   * A read-write lock for {@code subDataMap} of the block.
   * Let's set fairness option as true to prevent starvation.
   */
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

  /**
   * Executes sub operation on data keys assigned to this block.
   * All operations both from remote and local clients are executed via this method.
   */
  Map<Long, V> executeSubOperation(final RangeKeyOperation<Long, V> operation,
                                   final List<Pair<Long, Long>> keyRanges) {
    final DataOpType operationType = operation.getOpType();

    final Map<Long, V> outputData = new HashMap<>();
    switch (operationType) {
    case PUT:
      rwLock.writeLock().lock();
      try {
        final NavigableMap<Long, V> dataKeyValueMap = operation.getDataKVMap().get();
        for (final Pair<Long, Long> keyRange : keyRanges) {
          // extract matching entries from the input kv data map and put it all to subDataMap
          final NavigableMap<Long, V> subMap =
              dataKeyValueMap.subMap(keyRange.getFirst(), true, keyRange.getSecond(), true);
          subDataMap.putAll(subMap);
        }

        // PUT operations always succeed for all the ranges, because it overwrites map with the given input value.
        // So, outputData for PUT operations should be determined in other places.
      } finally {
        rwLock.writeLock().unlock();
      }
      break;
    case GET:
      rwLock.readLock().lock();
      try {
        for (final Pair<Long, Long> keyRange : keyRanges) {
          outputData.putAll(subDataMap.subMap(keyRange.getFirst(), true,
              keyRange.getSecond(), true));
        }
      } finally {
        rwLock.readLock().unlock();
      }
      break;
    case REMOVE:
      rwLock.writeLock().lock();
      try {
        for (final Pair<Long, Long> keyRange : keyRanges) {
          outputData.putAll(subDataMap.subMap(keyRange.getFirst(), true,
              keyRange.getSecond(), true));
        }
        subDataMap.keySet().removeAll(outputData.keySet());
      } finally {
        rwLock.writeLock().unlock();
      }
      break;
    default:
      throw new RuntimeException("Undefined operation");
    }

    return outputData;
  }

  @Override
  public Map<Long, V> getAll() {
    rwLock.readLock().lock();
    try {
      return (Map<Long, V>) ((TreeMap<Long, V>) subDataMap).clone();
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public void putAll(final Map<Long, V> toPut) {
    rwLock.writeLock().lock();
    try {
      subDataMap.putAll(toPut);
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  @Override
  public Map<Long, V> removeAll() {
    final Map<Long, V> result;
    rwLock.writeLock().lock();
    try {
      result = (Map<Long, V>) ((TreeMap<Long, V>) subDataMap).clone();
      subDataMap.clear();
    } finally {
      rwLock.writeLock().unlock();
    }

    return result;
  }

  @Override
  public int getNumPairs() {
    return subDataMap.size();
  }
}

