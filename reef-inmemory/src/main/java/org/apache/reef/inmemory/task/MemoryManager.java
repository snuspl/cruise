package org.apache.reef.inmemory.task;

import com.microsoft.tang.annotations.Parameter;
import org.apache.reef.inmemory.common.CacheStatistics;
import org.apache.reef.inmemory.common.CacheUpdates;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Memory manager keeps track of memory statistics and applies admission control
 * when there is too much memory pressure to load.
 * Memory usage is calculated as the amount of loading + pinned memory (memory that cannot be GC'd)
 * and memory usage is not allowed within slack of the total Heap size.
 */
public final class MemoryManager {

  private static final Logger LOG = Logger.getLogger(MemoryManager.class.getName());

  private final CacheStatistics statistics;
  private final int slack;
  private CacheUpdates updates;

  @Inject
  public MemoryManager(final CacheStatistics statistics,
                       final @Parameter(CacheParameters.HeapSlack.class) int slack) {
    this.statistics = statistics;
    this.slack = slack;
    this.updates = new CacheUpdates();
  }

  /**
   * Call before starting block loading.
   * Will wait for memory to free up if too much memory is being used for pin + load.
   * Updates statistics.
   * @param blockSize
   */
  public synchronized void loadStart(final long blockSize) {
    boolean canLoad = false;
    while (!canLoad) {

      final long maxHeap = statistics.getMaxBytes();
      final long loading = blockSize + statistics.getLoadingBytes();
      final long pinned = statistics.getPinnedBytes();
      canLoad = maxHeap - slack - loading - pinned > 0;

      if (!canLoad) {
        LOG.log(Level.WARNING, "Waiting to load block, maxHeap: " + maxHeap + ", loading: " + loading + ", pinned: " + pinned);
        try {
          wait();
        } catch (InterruptedException e) {
          LOG.log(Level.WARNING, "Wait interrupted", e);
        }
      }
    }
    statistics.addLoadingBytes(blockSize);
  }

  /**
   * Call on load success.
   * Notifies threads waiting for memory to free up.
   * Updates statistics.
   * @param blockSize
   * @param pinned
   */
  public synchronized void loadSuccess(final long blockSize, final boolean pinned) {
    statistics.subtractLoadingBytes(blockSize);

    if (pinned) {
      statistics.addPinnedBytes(blockSize);
    }
    statistics.addCacheBytes(blockSize);
    notifyAll();
  }

  /**
   * Call on load failure.
   * Notifies threads waiting for memory to free up.
   * Updates statistics.
   * @param blockSize
   */
  public synchronized void loadFail(final BlockId blockId, final Exception exception) {
    updates.addFailure(blockId, exception);
    statistics.subtractLoadingBytes(blockId.getBlockSize());
    notifyAll();
  }

  /**
   * Call on cache removal (eviction).
   * Notifies threads waiting for memory to free up.
   * Updates statistics.
   * @param blockId
   */
  public synchronized void remove(final BlockId blockId) {
    final long blockSize = blockId.getBlockSize();
    updates.addRemoval(blockId);
    statistics.subtractCacheBytes(blockSize);
    statistics.addEvictedBytes(blockSize);
    notifyAll();
  }

  /**
   * Call on pin cache removal.
   * No actual memory gets freed up on pin removal, so no notification is given.
   * Updates statistics.
   * @param blockSize
   */
  public synchronized void removePin(final long blockSize) {
    statistics.subtractPinnedBytes(blockSize);
  }

  /**
   * Clear statistics related to history, e.g. bytes evicted
   */
  public synchronized void clearHistory() {
    statistics.resetEvictedBytes();
  }

  public CacheStatistics getStatistics() {
    return statistics;
  }

  /**
   * Pull the cache updates. The returned updates are removed.
   * @return The latest updates from the cache Task
   */
  public synchronized CacheUpdates pullUpdates() {
    final CacheUpdates current = this.updates;
    this.updates = new CacheUpdates();
    return current;
  }
}
