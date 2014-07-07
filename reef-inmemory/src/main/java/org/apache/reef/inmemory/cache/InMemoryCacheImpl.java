package org.apache.reef.inmemory.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.inmemory.fs.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.fs.exceptions.BlockNotFoundException;

import javax.inject.Inject;
import java.nio.ByteBuffer;

/**
 * Implementation of Cache class using Google Cache interface. 
 */
public final class InMemoryCacheImpl implements InMemoryCache {
  private Cache<BlockId, byte[]> cache = null;
  private Cache<BlockId, Long> pending = null;

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
  public void read(final BlockId blockId, final ByteBuffer out, final long offset)
          throws BlockLoadingException, BlockNotFoundException {
    final Long pendingTime = pending.getIfPresent(blockId);
    if (pendingTime != null) {
      throw new BlockLoadingException(pendingTime);
    } else {
      final byte[] block = cache.getIfPresent(blockId);
      if (block == null) {
        throw new BlockNotFoundException();
      } else {
        out.put(block, (int)offset, out.capacity());
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
