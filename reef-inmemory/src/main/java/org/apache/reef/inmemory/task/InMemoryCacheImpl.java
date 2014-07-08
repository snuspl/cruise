package org.apache.reef.inmemory.task;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;

import javax.inject.Inject;
import java.nio.ByteBuffer;

/**
 * Implementation of Cache class using Google Cache interface. 
 */
public final class InMemoryCacheImpl implements InMemoryCache {
  private final Cache<BlockId, byte[]> cache;
  private final Cache<BlockId, Long> pending;

  @Inject
  public InMemoryCacheImpl() {
    cache = CacheBuilder.newBuilder()
        .concurrencyLevel(4)
        .build();
    pending = CacheBuilder.newBuilder()
        .concurrencyLevel(4)
        .build();
  }

  @Override
  public byte[] get(final BlockId blockId)
          throws BlockLoadingException, BlockNotFoundException {
    final Long pendingTime = pending.getIfPresent(blockId);
    if (pendingTime != null) {
      throw new BlockLoadingException(pendingTime);
    } else {
      final byte[] data = cache.getIfPresent(blockId);
      if (data == null) {
        throw new BlockNotFoundException();
      } else {
        return cache.getIfPresent(blockId);
      }
    }
  }

  @Override
  public void put(final BlockId blockId, final byte[] data) {
    cache.put(blockId, data);
    pending.invalidate(blockId);
  }

  @Override
  public void putPending(final BlockId blockId) {
    pending.put(blockId, System.currentTimeMillis());
  }

  @Override
  public void clear() {
    cache.invalidateAll();
  }

  @Override
  public CacheStats getReport() {
    return cache.stats();
  }
}
