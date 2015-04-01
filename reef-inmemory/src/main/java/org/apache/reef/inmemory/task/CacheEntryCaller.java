package org.apache.reef.inmemory.task;

import org.apache.reef.inmemory.common.BlockId;

import java.util.concurrent.Callable;

/**
 * Passed as the Callable loader for the main Guava cache in InMemoryCacheImpl.
 */
public final class CacheEntryCaller implements Callable<CacheEntry> {

  private final CacheEntry entry;
  private final MemoryManager memoryManager;

  public CacheEntryCaller(final CacheEntry entry,
                          final MemoryManager memoryManager) {
    this.entry = entry;
    this.memoryManager = memoryManager;
  }

  /**
   * This call will load a reference to the loader in the soft cache.
   * The actual loading is not initiated here, but rather by the CacheEntryExecutor.
   * The call to pinCache will pin the CacheEntry, by adding it to a companion strong referenced cache.
   */
  @Override
  public CacheEntry call() throws Exception {
    final BlockId blockId = entry.getBlockId();
    final long blockSize = entry.getBlockSize();
    final boolean pin = entry.isPinned();
    memoryManager.cacheInsert(blockId, blockSize, pin);
    return entry;
  }
}
