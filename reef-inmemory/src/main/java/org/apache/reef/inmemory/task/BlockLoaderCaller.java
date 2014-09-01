package org.apache.reef.inmemory.task;

import com.google.common.cache.Cache;

import java.util.concurrent.Callable;

/**
 * Passed as the Callable loader for the main Guava cache in InMemoryCacheImpl.
 */
public final class BlockLoaderCaller implements Callable<BlockLoader> {

  private final BlockLoader loader;
  private final boolean pin;
  private final Cache<BlockId, BlockLoader> pinCache;
  private final MemoryManager memoryManager;

  public BlockLoaderCaller(final BlockLoader loader,
                           final boolean pin,
                           final Cache<BlockId, BlockLoader> pinCache,
                           final MemoryManager memoryManager) {
    this.loader = loader;
    this.pin = pin;
    this.pinCache = pinCache;
    this.memoryManager = memoryManager;
  }

  /**
   * This call will load a reference to the loader in the soft cache.
   * The actual loading is not initiated here, but rather by the BlockLoaderExecutor.
   * The call to pinCache will pin the BlockLoader, by adding it to a companion strong referenced cache.
   */
  @Override
  public BlockLoader call() throws Exception {
    final BlockId blockId = loader.getBlockId();
    if (pin) {
      pinCache.put(blockId, loader);
    }
    if (memoryManager.getEvictionList())
    memoryManager.cacheInsert(blockId);
    return loader;
  }
}
