package org.apache.reef.inmemory.cache.hdfs;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.inmemory.cache.BlockId;
import org.apache.reef.inmemory.cache.InMemoryCache;

import javax.inject.Inject;
import java.nio.ByteBuffer;

/**
 * Implementation of Cache class using Google Cache interface. 
 */
public final class HdfsCache implements InMemoryCache {
  private Cache<BlockId, ByteBuffer> cache = null;
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();

  @Inject
  public HdfsCache() {
    cache = CacheBuilder.newBuilder()
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
