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
import java.util.concurrent.locks.Lock;

/**
 * An implementation of {@link Table}.
 * It provides a shared table interface to users, by providing transparent access to distributed tablets.
 * Specifically, it routes accesses to local tablet or remote executors that own other tablets
 * based on the block ownership status using {@link OwnershipCache}.
 */
@EvaluatorSide
@ThreadSafe
public final class TableImpl<K, V, U> implements Table<K, V, U> {
  /**
   * Table identifier.
   */
  private final String tableId;

  /**
   * Local cache for ownership mapping.
   */
  private final OwnershipCache ownershipCache;

  /**
   * A tablet which means an abstraction of local portion of table.
   */
  private final Tablet<K, V, U> tablet;

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
                    final Tablet<K, V, U> tablet,
                    final RemoteAccessOpSender remoteAccessOpSender,
                    final BlockPartitioner<K> blockPartitioner) {
    this.tableId = tableId;
    this.ownershipCache = ownershipCache;
    this.tablet = tablet;
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
        return tablet.put(blockId, key, value);
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
        final V result = tablet.put(blockId, key, value);
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
  public V putIfAbsent(final K key, final V value) {
    final int blockId = blockPartitioner.getBlockId(key);
    final Optional<String> remoteIdOptional;

    final Pair<Optional<String>, Lock> remoteIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
    try {
      remoteIdOptional = remoteIdWithLock.getKey();

      // execute operation in local, holding ownershipLock
      if (!remoteIdOptional.isPresent()) {
        return tablet.putIfAbsent(blockId, key, value);
      }
    } catch (final BlockNotExistsException e) {
      throw new RuntimeException(e);
    } finally {
      final Lock ownershipLock = remoteIdWithLock.getValue();
      ownershipLock.unlock();
    }

    // send operation to remote and wait until operation is finished
    final DataOpResult<V> opResult = remoteAccessOpSender.sendOpToRemote(
        OpType.PUT_IF_ABSENT, tableId, blockId, key, value, null, remoteIdOptional.get(), true);

    return opResult.getOutputData();
  }

  @Override
  public DataOpResult<V> putIfAbsentAsync(final K key, final V value) {
    final int blockId = blockPartitioner.getBlockId(key);
    final Optional<String> remoteIdOptional;

    final Pair<Optional<String>, Lock> remoteIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
    try {
      remoteIdOptional = remoteIdWithLock.getKey();

      // execute operation in local, holding ownershipLock
      if (!remoteIdOptional.isPresent()) {
        final V result = tablet.putIfAbsent(blockId, key, value);
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
        OpType.PUT_IF_ABSENT, tableId, blockId, key, value, null, remoteIdOptional.get(), false);

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
        return tablet.get(blockId, key);
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
        final V result = tablet.get(blockId, key);
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
        return tablet.update(blockId, key, updateValue);
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
        final V result = tablet.update(blockId, key, updateValue);
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
        return tablet.remove(blockId, key);
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
        final V result = tablet.remove(blockId, key);
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
  public Tablet<K, V, U> getLocalTablet() {
    return tablet;
  }
}
