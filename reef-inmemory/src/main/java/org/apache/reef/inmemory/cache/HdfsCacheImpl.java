package org.apache.reef.inmemory.cache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

/**
 * Implementation of Cache class using Google Cache interface. 
 */
public class HdfsCacheImpl implements InMemoryCache {
  private Cache<BlockId, ByteBuffer> cache = null;
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();

  public HdfsCacheImpl() {
    cache = CacheBuilder.newBuilder()
        .maximumSize(100L)
        .expireAfterAccess(10, TimeUnit.HOURS)
        .concurrencyLevel(4)
        .build();
  }

  @Override
  public ByteBuffer get(BlockId blockId) {
    return cache.getIfPresent(blockId);
  }

  @Override
  public void put(BlockId blockId, ByteBuffer buffer) {
    cache.put(blockId, buffer);
  }

  @Override
  public void clear() {
    cache.invalidateAll();
  }

  @Override
  public byte[] getReport() {
    return CODEC.encode(cache.stats().toString());
  }
}
