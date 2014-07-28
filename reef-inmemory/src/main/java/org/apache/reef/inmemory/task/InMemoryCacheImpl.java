package org.apache.reef.inmemory.task;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.reef.inmemory.common.CacheStatistics;
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

  private final CacheStatistics statistics = new CacheStatistics();

  @Inject
  public InMemoryCacheImpl() {
    final int concurrencyLevel = 2; // synchronized ops (1)  + statistics (1) = 2
    cache = CacheBuilder.newBuilder()
        .concurrencyLevel(concurrencyLevel)
        .build();
    loading = CacheBuilder.newBuilder()
        .concurrencyLevel(concurrencyLevel)
        .build();
  }

  @Override
  public synchronized byte[] get(final BlockId blockId)
          throws BlockLoadingException, BlockNotFoundException {
    final BlockLoader activeLoader = loading.getIfPresent(blockId);
    if (activeLoader != null) {
      throw new BlockLoadingException(activeLoader.getBytesLoaded());
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

        statistics.addLoadingMB((int)blockId.getBlockSize());
      }
    }

    final byte[] data = blockLoader.loadBlock();

    synchronized (this) {
      statistics.subtractLoadingMB((int)blockId.getBlockSize());

      if (loading.getIfPresent(blockId) == null) {
        LOG.log(Level.WARNING, "Block load completed but no longer needed "+blockId);
      } else {
        loading.invalidate(blockId);

        cache.put(blockId, data);
        statistics.addCacheMB((int) blockId.getBlockSize());
      }
    }
  }

  @Override
  public synchronized void clear() {
    cache.invalidateAll();

    statistics.resetCacheMB();
  }

  @Override
  public synchronized CacheStatistics getStatistics() {
    return statistics;
  }
}
