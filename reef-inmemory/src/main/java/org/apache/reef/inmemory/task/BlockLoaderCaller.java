package org.apache.reef.inmemory.task;

import com.google.common.cache.Cache;

import java.util.concurrent.Callable;

/**
 * Passed as the Callable loader for the main Guava cache in InMemoryCacheImpl.
 */
public final class BlockLoaderCaller implements Callable<BlockLoader> {

  private final BlockLoader loader;
  private final boolean pin;
  private Cache<BlockId, BlockLoader> pinCache;

  public BlockLoaderCaller(final BlockLoader loader,
                           final boolean pin,
                           final Cache<BlockId, BlockLoader> pinCache) {
    this.loader = loader;
    this.pin = pin;
    this.pinCache = pinCache;
  }

  /**
   * This call will load a reference to the loader in the soft cache, but will not actually start the load.
   * The call to pinCache will pin the BlockLoader, by adding it to a companion strong referenced cache.
   */
  @Override
  public BlockLoader call() throws Exception {
    if (pin) {
      pinCache.put(loader.getBlockId(), loader);
    }
    return loader;
  }
}
