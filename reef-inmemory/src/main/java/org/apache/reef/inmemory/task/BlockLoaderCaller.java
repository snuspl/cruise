package org.apache.reef.inmemory.task;

import org.apache.reef.inmemory.common.BlockId;

import java.util.concurrent.Callable;

/**
 * Passed as the Callable loader for the main Guava cache in InMemoryCacheImpl.
 */
public final class BlockLoaderCaller implements Callable<BlockLoader> {

  private final BlockLoader loader;
  private final MemoryManager memoryManager;

  public BlockLoaderCaller(final BlockLoader loader,
                           final MemoryManager memoryManager) {
    this.loader = loader;
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
    final long blockSize = loader.getBlockSize();
    final boolean pin = loader.isPinned();
    memoryManager.cacheInsert(blockId, blockSize, pin);
    return loader;
  }
}
