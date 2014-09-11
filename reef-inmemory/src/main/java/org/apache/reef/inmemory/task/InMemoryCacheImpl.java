package org.apache.reef.inmemory.task;

import com.google.common.cache.*;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EStage;
import org.apache.reef.inmemory.common.CacheStatistics;
import org.apache.reef.inmemory.common.CacheUpdates;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;

import javax.inject.Inject;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of Cache class using Google Cache interface. 
 */
public final class InMemoryCacheImpl implements InMemoryCache {
  private final Logger LOG = Logger.getLogger(InMemoryCacheImpl.class.getName());

  private final MemoryManager memoryManager;
  private final LRUEvictionManager lru;
  private final EStage<BlockLoader> loadingStage;
  private final int loadingBufferSize;

  private final Cache<BlockId, BlockLoader> cache;

  @Inject
  public InMemoryCacheImpl(final Cache<BlockId, BlockLoader> cache,
                           final MemoryManager memoryManager,
                           final LRUEvictionManager lru,
                           final EStage<BlockLoader> loadingStage,
                           final @Parameter(CacheParameters.NumServerThreads.class) int numThreads,
                           final @Parameter(CacheParameters.LoadingBufferSize.class) int loadingBufferSize) {
    this.cache = cache;
    this.memoryManager = memoryManager;
    this.lru = lru;
    this.loadingStage = loadingStage;
    this.loadingBufferSize = loadingBufferSize;
  }

  @Override
  public byte[] get(final BlockId blockId, int index)
          throws BlockLoadingException, BlockNotFoundException {
    final BlockLoader loader = cache.getIfPresent(blockId);
    if (loader == null) {
      throw new BlockNotFoundException();
    } else {
      lru.use(blockId);
      // getData throws BlockLoadingException if load has not completed for the requested chunk
      return loader.getData(index);
    }
  }

  @Override
  public void load(final BlockLoader loader) throws IOException {
    final Callable<BlockLoader> callable = new BlockLoaderCaller(loader, memoryManager);
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
  public int getLoadingBufferSize() {
    return loadingBufferSize;
  }

  @Override
  public void clear() { // TODO: do we need a stage for this as well? For larger caches, it could take awhile
    final List<BlockId> blockIds = lru.evictAll(true); // TODO add CLI options for evicting all except for pinned
    for (final BlockId blockId : blockIds) {
      cache.invalidate(blockId);
    }
    cache.cleanUp();
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
