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
import edu.snu.cay.services.et.configuration.parameters.KeyCodec;
import edu.snu.cay.services.et.configuration.parameters.TableIdentifier;
import edu.snu.cay.services.et.evaluator.api.*;
import edu.snu.cay.services.et.exceptions.BlockNotExistsException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.Future;
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
   * A key codec.
   */
  private final Codec<K> keyCodec;

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
                    @Parameter(KeyCodec.class) final Codec<K> keyCodec,
                    final OwnershipCache ownershipCache,
                    final Tablet<K, V, U> tablet,
                    final RemoteAccessOpSender remoteAccessOpSender,
                    final BlockPartitioner<K> blockPartitioner) {
    this.tableId = tableId;
    this.keyCodec = keyCodec;
    this.ownershipCache = ownershipCache;
    this.tablet = tablet;
    this.remoteAccessOpSender = remoteAccessOpSender;
    this.blockPartitioner = blockPartitioner;
  }

  @Override
  public DataOpResult<V> put(final K key, final V value) {
    return putInternal(key, value, true);
  }

  @Override
  public void putNoReply(final K key, final V value) {
    putInternal(key, value, false);
  }

  private DataOpResult<V> putInternal(final K key, final V value, final boolean replyRequired) {
    final EncodedKey<K> encodedKey = new EncodedKey<>(key, keyCodec);

    final int blockId = blockPartitioner.getBlockId(encodedKey);
    final Optional<String> remoteIdOptional;

    final Pair<Optional<String>, Lock> remoteIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
    try {
      remoteIdOptional = remoteIdWithLock.getKey();

      // execute operation in local, holding ownershipLock
      if (!remoteIdOptional.isPresent()) {
        final V result = tablet.put(blockId, key, value);
        return new SingleKeyDataOpResult<>(result, true);
      } else {
        final DataOpResult<V> dataOpResult = new SingleKeyDataOpResult<>();
        // send operation to remote
        remoteAccessOpSender.sendSingleKeyOpToRemote(
            OpType.PUT, tableId, blockId, key, value, null,
            remoteIdOptional.get(), replyRequired, dataOpResult);

        return dataOpResult;
      }
    } catch (final BlockNotExistsException e) {
      throw new RuntimeException(e);
    } finally {
      final Lock ownershipLock = remoteIdWithLock.getValue();
      ownershipLock.unlock();
    }
  }

  @Override
  public DataOpResult<V> putIfAbsent(final K key, final V value) {
    return putIfAbsentInternal(key, value, true);
  }

  @Override
  public void putIfAbsentNoReply(final K key, final V value) {
    putIfAbsentInternal(key, value, false);
  }

  @Override
  public DataOpResult<Map<K, V>> multiPut(final List<Pair<K, V>> kvList) {

    final Map<Integer, List<Pair<K, V>>> blockToPairListMap = new HashMap<>();
    for (final Pair<K, V> kvPair : kvList) {
      final K key = kvPair.getLeft();
      final int blockId = blockPartitioner.getBlockId(key);
      blockToPairListMap.putIfAbsent(blockId, new ArrayList<>());
      blockToPairListMap.get(blockId).add(kvPair);
    }

    final DataOpResult<Map<K, V>> aggregateDataOpResult = new MultiKeyDataOpResult<>(blockToPairListMap.size());
    blockToPairListMap.forEach((blockId, kvPairList) -> {
      final Pair<Optional<String>, Lock> remoteIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
      final Optional<String> remoteIdOptional;
      remoteIdOptional = remoteIdWithLock.getKey();
      try {
        // execute operation in local
        if (!remoteIdOptional.isPresent()) {
          final Map<K, V> localResultMap = new HashMap<>();
          for (final Pair<K, V> pair : kvPairList) {
            final V output = tablet.put(blockId, pair.getKey(), pair.getValue());
            if (output != null) {
              localResultMap.put(pair.getKey(), output);
            }
          }
          aggregateDataOpResult.onCompleted(localResultMap, true);
        } else {

          final List<K> keyList = new ArrayList<>(kvPairList.size());
          final List<V> valueList = new ArrayList<>(kvPairList.size());
          kvPairList.forEach(pair -> {
            keyList.add(pair.getKey());
            valueList.add(pair.getValue());
          });

          // send operation to remote
          remoteAccessOpSender.sendMultiKeyOpToRemote(OpType.PUT, tableId, blockId, keyList, valueList,
              Collections.emptyList(), remoteIdOptional.get(), true, aggregateDataOpResult);
        }
      } catch (BlockNotExistsException e) {
        throw new RuntimeException(e);
      } finally {
        final Lock ownershipLock = remoteIdWithLock.getValue();
        ownershipLock.unlock();
      }
    });

    return aggregateDataOpResult;
  }

  private DataOpResult<V> putIfAbsentInternal(final K key, final V value, final boolean replyRequired) {
    final EncodedKey<K> encodedKey = new EncodedKey<>(key, keyCodec);

    final int blockId = blockPartitioner.getBlockId(encodedKey);
    final Optional<String> remoteIdOptional;

    final Pair<Optional<String>, Lock> remoteIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
    try {
      remoteIdOptional = remoteIdWithLock.getKey();

      // execute operation in local, holding ownershipLock
      if (!remoteIdOptional.isPresent()) {
        final V result = tablet.putIfAbsent(blockId, key, value);
        return new SingleKeyDataOpResult<>(result, true);
      } else {

        final DataOpResult<V> dataOpResult = new SingleKeyDataOpResult<>();
        // send operation to remote
        remoteAccessOpSender.sendSingleKeyOpToRemote(
            OpType.PUT_IF_ABSENT, tableId, blockId, key, value, null,
            remoteIdOptional.get(), replyRequired, dataOpResult);
        return dataOpResult;
      }
    } catch (final BlockNotExistsException e) {
      throw new RuntimeException(e);
    } finally {
      final Lock ownershipLock = remoteIdWithLock.getValue();
      ownershipLock.unlock();
    }

  }

  @Override
  public DataOpResult<V> get(final K key) {
    final EncodedKey<K> encodedKey = new EncodedKey<>(key, keyCodec);

    final int blockId = blockPartitioner.getBlockId(encodedKey);
    final Optional<String> remoteIdOptional;

    final Pair<Optional<String>, Lock> remoteIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
    try {
      remoteIdOptional = remoteIdWithLock.getKey();

      // execute operation in local, holding ownershipLock
      if (!remoteIdOptional.isPresent()) {
        final V result = tablet.get(blockId, key);
        return new SingleKeyDataOpResult<>(result, true);
      } else {
        final DataOpResult<V> dataOpResult = new SingleKeyDataOpResult<>();
        // send operation to remote
        remoteAccessOpSender.sendSingleKeyOpToRemote(
            OpType.GET, tableId, blockId, key, null, null,
            remoteIdOptional.get(), true, dataOpResult);

        return dataOpResult;
      }
    } catch (final BlockNotExistsException e) {
      throw new RuntimeException(e);
    } finally {
      final Lock ownershipLock = remoteIdWithLock.getValue();
      ownershipLock.unlock();
    }
  }

  @Override
  public Future<V> getOrInit(final K key) {
    final EncodedKey<K> encodedKey = new EncodedKey<>(key, keyCodec);

    final int blockId = blockPartitioner.getBlockId(encodedKey);
    final Optional<String> remoteIdOptional;

    final Pair<Optional<String>, Lock> remoteIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
    try {
      remoteIdOptional = remoteIdWithLock.getKey();

      // execute operation in local, holding ownershipLock
      if (!remoteIdOptional.isPresent()) {
        final V result = tablet.getOrInit(blockId, key);
        return new SingleKeyDataOpResult<>(result, true);
      } else {
        final DataOpResult<V> dataOpResult = new SingleKeyDataOpResult<>();
        // send operation to remote
        remoteAccessOpSender.sendSingleKeyOpToRemote(
            OpType.GET_OR_INIT, tableId, blockId, key, null, null,
            remoteIdOptional.get(), true, dataOpResult);

        return dataOpResult;
      }
    } catch (final BlockNotExistsException e) {
      throw new RuntimeException(e);
    } finally {
      final Lock ownershipLock = remoteIdWithLock.getValue();
      ownershipLock.unlock();
    }

  }

  @Override
  public DataOpResult<V> update(final K key, final U updateValue) {
    return updateInternal(key, updateValue, true);
  }

  @Override
  public Future<Map<K, V>> multiUpdate(final List<Pair<K, U>> kuList) {

    final Map<Integer, List<Pair<K, U>>> blockToPairListMap = new HashMap<>();
    for (final Pair<K, U> kuPair : kuList) {
      final K key = kuPair.getLeft();
      final int blockId = blockPartitioner.getBlockId(key);
      blockToPairListMap.putIfAbsent(blockId, new ArrayList<>());
      blockToPairListMap.get(blockId).add(kuPair);
    }

    final DataOpResult<Map<K, V>> aggregateDataOpResult = new MultiKeyDataOpResult<>(blockToPairListMap.size());
    blockToPairListMap.forEach((blockId, kuPairList) -> {
      final Pair<Optional<String>, Lock> remoteIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
      final Optional<String> remoteIdOptional;
      remoteIdOptional = remoteIdWithLock.getKey();
      try {
        // execute operation in local
        if (!remoteIdOptional.isPresent()) {
          final Map<K, V> localResultMap = new HashMap<>();
          for (final Pair<K, U> pair : kuPairList) {
            final V output = tablet.update(blockId, pair.getKey(), pair.getValue());
            if (output != null) {
              localResultMap.put(pair.getKey(), output);
            }
          }
          aggregateDataOpResult.onCompleted(localResultMap, true);
        } else {

          final List<K> keyList = new ArrayList<>(kuPairList.size());
          final List<U> updateValueList = new ArrayList<>(kuPairList.size());
          kuPairList.forEach(pair -> {
            keyList.add(pair.getKey());
            updateValueList.add(pair.getValue());
          });

          // send operation to remote
          remoteAccessOpSender.sendMultiKeyOpToRemote(OpType.UPDATE, tableId, blockId, keyList, Collections.emptyList(),
              updateValueList, remoteIdOptional.get(), true, aggregateDataOpResult);
        }
      } catch (BlockNotExistsException e) {
        throw new RuntimeException(e);
      } finally {
        final Lock ownershipLock = remoteIdWithLock.getValue();
        ownershipLock.unlock();
      }
    });

    return aggregateDataOpResult;
  }

  @Override
  public void updateNoReply(final K key, final U updateValue) {
    updateInternal(key, updateValue, false);
  }

  private DataOpResult<V> updateInternal(final K key, final U updateValue, final boolean replyRequired) {
    final EncodedKey<K> encodedKey = new EncodedKey<>(key, keyCodec);

    final int blockId = blockPartitioner.getBlockId(encodedKey);
    final Optional<String> remoteIdOptional;

    final Pair<Optional<String>, Lock> remoteIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
    try {
      remoteIdOptional = remoteIdWithLock.getKey();

      // execute operation in local, holding ownershipLock
      if (!remoteIdOptional.isPresent()) {
        final V result = tablet.update(blockId, key, updateValue);
        return new SingleKeyDataOpResult<>(result, true);
      } else {
        final DataOpResult<V> dataOpResult = new SingleKeyDataOpResult<>();
        // send operation to remote
        remoteAccessOpSender.sendSingleKeyOpToRemote(
            OpType.UPDATE, tableId, blockId, key, null,
            updateValue, remoteIdOptional.get(), replyRequired, dataOpResult);

        return dataOpResult;
      }
    } catch (final BlockNotExistsException e) {
      throw new RuntimeException(e);
    } finally {
      final Lock ownershipLock = remoteIdWithLock.getValue();
      ownershipLock.unlock();
    }
  }

  @Override
  public DataOpResult<V> remove(final K key) {
    return removeInternal(key, true);
  }

  @Override
  public void removeNoReply(final K key) {
    removeInternal(key, false);
  }

  private DataOpResult<V> removeInternal(final K key, final boolean replyRequired) {
    final EncodedKey<K> encodedKey = new EncodedKey<>(key, keyCodec);

    final int blockId = blockPartitioner.getBlockId(encodedKey);
    final Optional<String> remoteIdOptional;

    final Pair<Optional<String>, Lock> remoteIdWithLock = ownershipCache.resolveExecutorWithLock(blockId);
    try {
      remoteIdOptional = remoteIdWithLock.getKey();

      // execute operation in local, holding ownershipLock
      if (!remoteIdOptional.isPresent()) {
        final V result = tablet.remove(blockId, key);
        return new SingleKeyDataOpResult<>(result, true);
      } else {
        final DataOpResult<V> dataOpResult = new SingleKeyDataOpResult<>();
        // send operation to remote
        remoteAccessOpSender.sendSingleKeyOpToRemote(
            OpType.REMOVE, tableId, blockId, key, null,
            null, remoteIdOptional.get(), replyRequired, dataOpResult);

        return dataOpResult;
      }
    } catch (final BlockNotExistsException e) {
      throw new RuntimeException(e);
    } finally {
      final Lock ownershipLock = remoteIdWithLock.getValue();
      ownershipLock.unlock();
    }
  }

  @Override
  public Tablet<K, V, U> getLocalTablet() {
    return tablet;
  }
}
