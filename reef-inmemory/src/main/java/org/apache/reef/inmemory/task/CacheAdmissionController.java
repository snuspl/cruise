package org.apache.reef.inmemory.task;

import com.google.common.cache.Cache;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;
import org.apache.reef.inmemory.common.exceptions.MemoryLimitException;

import javax.inject.Inject;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class controls admission into the cache.
 * If the memory is not enough to accommodate the block,
 * then secure the space via eviction.
 */
public class CacheAdmissionController {
  private static final Logger LOG = Logger.getLogger(CacheAdmissionController.class.getName());

  private MemoryManager memoryManager;
  private Cache<BlockId, BlockLoader> cache;

  @Inject
  public CacheAdmissionController(final MemoryManager memoryManager,
                                  final Cache<BlockId, BlockLoader> cache) {
    this.cache = cache;
    this.memoryManager = memoryManager;
  }

  /**
   * Reserve memory space to load the block. In case the capacity is not enough to load,
   * then it preserve the memory via eviction. If it is done successfully, MemoryManager
   * is notified that the block is on the loading state.
   * @param blockId Block Id to load
   * @param pin Whether the block is to be pinned
   * @throws MemoryLimitException If it is not available to reserve even with eviction.
   * @throws BlockNotFoundException If the metadata is not found in the cache.
   */
  public synchronized void reserveSpace(final BlockId blockId, final boolean pin) throws MemoryLimitException, BlockNotFoundException {
    // 1. If the cache is full, an eviction list is returned by loadStart.
    // 2. Each block in the eviction list is invalidated here.
    // 3. loadStart must be called again; the Memory Manager then ensures that
    //    eviction has successfully taken place and been booked, and returns null if true.
    // TODO: there's a possibility of starvation here:
    //    Between 2 and 3, a new block can be inserted and obtain memory that has just been evicted.
    //    If blocks are continually inserted, this will starve the evicting block.
    boolean needSpace = true;
    while (needSpace) {
      final List<BlockId> evictionList = memoryManager.loadStart(blockId, pin);
      needSpace = (evictionList != null);
      if (needSpace) {
        for (final BlockId toEvict : evictionList) {
          LOG.log(Level.INFO, toEvict+" eviction request being made");
          cache.invalidate(toEvict);
        }
      }
    }
  }
}
