package org.apache.reef.inmemory.task;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EStage;
import org.apache.reef.inmemory.common.CacheStatistics;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;

import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of Cache class using Google Cache interface. 
 */
public final class InMemoryCacheImpl implements InMemoryCache {
  private final Logger LOG = Logger.getLogger(InMemoryCacheImpl.class.getName());

  private final CacheStatistics statistics;
  private final EStage<BlockLoader> loadingStage;

  private final Cache<BlockId, BlockLoader> cache;
  private final Cache<BlockId, BlockLoader> pinCache;

  @Inject
  public InMemoryCacheImpl(final CacheStatistics statistics,
                           final EStage<BlockLoader> loadingStage,
                           final @Parameter(CacheParameters.NumServerThreads.class) int numThreads) {
    cache = CacheBuilder.newBuilder()
            .softValues()
            .concurrencyLevel(numThreads)
            .build();
    pinCache = CacheBuilder.newBuilder()
            .concurrencyLevel(numThreads)
            .build();

    this.statistics = statistics;
    this.loadingStage = loadingStage;
  }

  @Override
  public byte[] get(final BlockId blockId)
          throws BlockLoadingException, BlockNotFoundException {
    final BlockLoader loader = cache.getIfPresent(blockId);
    if (loader == null) {
      throw new BlockNotFoundException();
    } else {
      // getData throws BlockLoadingException if load has not completed
      return loader.getData();
    }
  }

  @Override
  public void load(final BlockLoader loader, final boolean pin) throws IOException {
    final Callable<BlockLoader> callable = new BlockLoaderCaller(loader, pin, pinCache);
    final BlockLoader returnedLoader;
    try {
      returnedLoader = cache.get(loader.getBlockId(), callable);
    } catch (ExecutionException e) {
      throw new IOException(e);
    }

    // Only run loadBlock if our loader entered the cache
    if (loader == returnedLoader) {
      LOG.log(Level.INFO, "Add loading block {0}", loader.getBlockId());
      loadingStage.onNext(loader);
    }
  }

  @Override
  public void clear() {
    cache.invalidateAll();
    pinCache.invalidateAll();

    statistics.resetCacheBytes();
  }

  @Override
  public CacheStatistics getStatistics() {
    return statistics;
  }
}
