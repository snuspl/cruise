package org.apache.reef.inmemory.task;

import com.google.common.cache.*;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EStage;
import org.apache.reef.inmemory.common.CacheStatistics;
import org.apache.reef.inmemory.common.CacheUpdates;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;

import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of Cache class using Google Cache interface. 
 */
public final class InMemoryCacheImpl implements InMemoryCache {
  private final Logger LOG = Logger.getLogger(InMemoryCacheImpl.class.getName());

  private final MemoryManager memoryManager;
  private final LRUEvictionManager lruEvictionManager;
  private final EStage<BlockLoader> loadingStage;

  private final Cache<BlockId, BlockLoader> cache;
  private final Cache<BlockId, BlockLoader> pinCache;

  private final ScheduledExecutorService cleanupScheduler;

  /**
   * Clean up statistics, scheduled periodically
   */
  private final Runnable cleanup = new Runnable() {
    @Override
    public void run() {
      cache.cleanUp();
      pinCache.cleanUp();
    }
  };

  /**
   * Update statistics on cache removal
   */
  private final RemovalListener<BlockId, BlockLoader> pinRemovalListener = new RemovalListener<BlockId, BlockLoader>() {
    @Override
    public void onRemoval(RemovalNotification<BlockId, BlockLoader> notification) {
      LOG.log(Level.INFO, "Removed pin: "+notification.getKey());
      final long blockSize = notification.getKey().getBlockSize();
      memoryManager.removePin(blockSize);
    }
  };

  @Inject
  public InMemoryCacheImpl(final Cache<BlockId, BlockLoader> cache,
                           final MemoryManager memoryManager,
                           final LRUEvictionManager lruEvictionManager,
                           final EStage<BlockLoader> loadingStage,
                           final @Parameter(CacheParameters.NumServerThreads.class) int numThreads) {
    this.cache = cache;
    this.pinCache = CacheBuilder.newBuilder()
                    .removalListener(pinRemovalListener)
                    .concurrencyLevel(numThreads)
                    .build();

    this.memoryManager = memoryManager;
    this.lruEvictionManager = lruEvictionManager;
    this.loadingStage = loadingStage;

    this.cleanupScheduler = Executors.newScheduledThreadPool(1);
    this.cleanupScheduler.scheduleAtFixedRate(cleanup, 5, 5, TimeUnit.SECONDS);
  }

  @Override
  public byte[] get(final BlockId blockId)
          throws BlockLoadingException, BlockNotFoundException {
    lruEvictionManager.use(blockId);
    final BlockLoader loader = cache.getIfPresent(blockId);
    if (loader == null) {
      final BlockLoader pinLoader = pinCache.getIfPresent(blockId);
      if (pinLoader != null) {
        return pinLoader.getData();
      } else {
        throw new BlockNotFoundException();
      }
    } else {
      // getData throws BlockLoadingException if load has not completed
      return loader.getData();
    }
  }

  @Override
  public void load(final BlockLoader loader, final boolean pin) throws IOException {
    final Callable<BlockLoader> callable = new BlockLoaderCaller(loader, pin, pinCache, memoryManager);
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

    cache.cleanUp();
    pinCache.cleanUp();

    memoryManager.clearHistory();
  }

  @Override
  public CacheStatistics getStatistics() {
    return memoryManager.getStatistics();
  }

  @Override
  public CacheUpdates pullUpdates() {
    return memoryManager.pullUpdates();
  }
}
