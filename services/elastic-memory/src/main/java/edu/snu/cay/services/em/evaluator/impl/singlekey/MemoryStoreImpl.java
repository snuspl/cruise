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
import edu.snu.cay.services.em.evaluator.api.*;
import edu.snu.cay.services.em.evaluator.impl.BlockStore;
import edu.snu.cay.services.em.evaluator.impl.OperationRouter;
import edu.snu.cay.utils.trace.HTrace;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.util.Optional;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.locks.Lock;

/**
 * A {@code MemoryStore} implementation for a key of generic type, non-supporting range operations.
 * It routes operations to local {@link BlockStore} or remote through {@link RemoteOpHandler}
 * based on the routing result from {@link OperationRouter}.
 * Assuming EM applications always need to instantiate this class, HTrace initialization is done in the constructor.
 */
public final class MemoryStoreImpl<K> implements MemoryStore<K> {
  private final OperationRouter router;
  private final BlockResolver<K> blockResolver;
  private final RemoteOpHandlerImpl<K> remoteOpHandlerImpl;
  private final BlockStore blockStore;

  @Inject
  private MemoryStoreImpl(final HTrace hTrace,
                          final OperationRouter router,
                          final BlockResolver<K> blockResolver,
                          final RemoteOpHandlerImpl<K> remoteOpHandlerImpl,
                          final BlockStore blockStore) {
    hTrace.initialize();
    this.router = router;
    this.blockResolver = blockResolver;
    this.remoteOpHandlerImpl = remoteOpHandlerImpl;
    this.blockStore = blockStore;
  }

  @Override
  public boolean registerBlockUpdateListener(final BlockUpdateListener listener) {
    return blockStore.registerBlockUpdateListener(listener);
  }

  @Override
  public <V> Pair<K, Boolean> put(final K id, @Nonnull final V value) {

    final int blockId = blockResolver.resolveBlock(id);
    final Optional<String> remoteEvalIdOptional;

    final Tuple<Optional<String>, Lock> remoteEvalIdWithLock = router.resolveEvalWithLock(blockId);
    try {
      remoteEvalIdOptional = remoteEvalIdWithLock.getKey();

      // execute operation in local, holding routerLock
      if (!remoteEvalIdOptional.isPresent()) {
        final BlockImpl<K, V> block = (BlockImpl<K, V>) blockStore.get(blockId);
        block.put(id, value);
        return new Pair<>(id, true);
      }
    } finally {
      final Lock routerLock = remoteEvalIdWithLock.getValue();
      routerLock.unlock();
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

    final Tuple<Optional<String>, Lock> remoteEvalIdWithLock = router.resolveEvalWithLock(blockId);
    try {
      remoteEvalIdOptional = remoteEvalIdWithLock.getKey();

      // execute operation in local, holding routerLock
      if (!remoteEvalIdOptional.isPresent()) {
        final BlockImpl<K, V> block = (BlockImpl<K, V>) blockStore.get(blockId);
        final V output = block.get(id);
        return output == null ? null : new Pair<>(id, output);
      }
    } finally {
      final Lock routerLock = remoteEvalIdWithLock.getValue();
      routerLock.unlock();
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

    final List<Integer> localBlockIds = router.getCurrentLocalBlockIds();
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

    final Tuple<Optional<String>, Lock> remoteEvalIdWithLock = router.resolveEvalWithLock(blockId);
    try {
      remoteEvalIdOptional = remoteEvalIdWithLock.getKey();

      // execute operation in local, holding routerLock
      if (!remoteEvalIdOptional.isPresent()) {
        final BlockImpl<K, V> block = (BlockImpl<K, V>) blockStore.get(blockId);
        final V output = block.update(id, deltaValue);
        return new Pair<>(id, output);
      }
    } finally {
      final Lock routerLock = remoteEvalIdWithLock.getValue();
      routerLock.unlock();
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

    final Tuple<Optional<String>, Lock> remoteEvalIdWithLock = router.resolveEvalWithLock(blockId);
    try {
      remoteEvalIdOptional = remoteEvalIdWithLock.getKey();

      // execute operation in local, holding routerLock
      if (!remoteEvalIdOptional.isPresent()) {
        final BlockImpl<K, V> block = (BlockImpl<K, V>) blockStore.get(blockId);
        final V output = block.remove(id);
        return output == null ? null : new Pair<>(id, output);
      }
    } finally {
      final Lock routerLock = remoteEvalIdWithLock.getValue();
      routerLock.unlock();
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

    final List<Integer> localBlockIds = router.getCurrentLocalBlockIds();
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
    return router.resolveEval(blockId);
  }
}
