package org.apache.reef.inmemory.task;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of Cache class using Google Cache interface. 
 */
public final class InMemoryCacheImpl implements InMemoryCache {
  private final Logger LOG = Logger.getLogger(InMemoryCacheImpl.class.getName());

  private final Cache<BlockId, byte[]> cache;
  private final Cache<BlockId, BlockLoader> loading;

  @Inject
  public InMemoryCacheImpl() {
    cache = CacheBuilder.newBuilder()
        .concurrencyLevel(4)
        .build();
    loading = CacheBuilder.newBuilder()
        .concurrencyLevel(4)
        .build();
  }

  @Override
  public synchronized byte[] get(final BlockId blockId)
          throws BlockLoadingException, BlockNotFoundException {
    if (loading.getIfPresent(blockId) != null) {
      throw new BlockLoadingException(); // TODO: add block load start time
    } else {
      final byte[] data = cache.getIfPresent(blockId);
      if (data == null) {
        throw new BlockNotFoundException();
      } else {
        return data;
      }
    }
  }

  @Override
  public void load(BlockLoader blockLoader) throws IOException {
    final BlockId blockId = blockLoader.getBlockId();
    synchronized (this) {
      if (loading.getIfPresent(blockId) != null) {
        LOG.log(Level.WARNING, "Block load request for already loading block "+blockId);
        return;
      } else {
        loading.put(blockId, blockLoader);
      }
    }

    final byte[] data = blockLoader.loadBlock();

    synchronized (this) {
      if (loading.getIfPresent(blockId) == null) {
        LOG.log(Level.WARNING, "Block load completed but no longer needed "+blockId);
      } else {
        cache.put(blockId, data);
        loading.invalidate(blockId);
      }
    }
  }

  @Override
  public synchronized void clear() {
    cache.invalidateAll();
  }

  @Override
  public CacheStats getReport() {
    return cache.stats();
  }
}
