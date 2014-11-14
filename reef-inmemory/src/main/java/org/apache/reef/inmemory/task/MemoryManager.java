package org.apache.reef.inmemory.task;

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.inmemory.common.CacheStatistics;
import org.apache.reef.inmemory.common.CacheUpdates;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;
import org.apache.reef.inmemory.common.exceptions.MemoryLimitException;

import javax.inject.Inject;
import java.util.*;
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

  private final LRUEvictionManager lru;
  private final CacheStatistics statistics;
  private final long cacheSize;
  private CacheUpdates updates;

  private Map<BlockId, CacheEntryState> cacheEntries = new HashMap<>();

  @Inject
  public MemoryManager(final LRUEvictionManager lru,
                       final CacheStatistics statistics,
                       final @Parameter(CacheParameters.HeapSlack.class) double slack) {
    this.lru = lru;
    this.statistics = statistics;
    this.updates = new CacheUpdates();
    this.cacheSize = (long)(statistics.getMaxBytes() * (1.0 - slack));
  }

  private static enum CacheEntryState {
    INSERTED,
    LOAD_STARTED,
    LOAD_SUCCEEDED,
    LOAD_FAILED,
    REMOVED_DURING_LOAD,
    REMOVED
  }

  /**
   * Call during cache insert call.
   */
  public synchronized void cacheInsert(final BlockId blockId, final boolean pin) {
    if (isState(blockId, CacheEntryState.INSERTED)) {
      throw new RuntimeException(blockId+" has been previously inserted");
    }
    final long blockSize = blockId.getBlockSize();

    final long cached = statistics.getCacheBytes();
    if (cached < 0) {
      throw new RuntimeException(blockId+" cached is less than zero: "+cached);
    }

    if (pin) {
      statistics.addPinnedBytes(blockSize);
      lru.addPinned(blockId);
    } else {
      lru.add(blockId);
    }

    setState(blockId, CacheEntryState.INSERTED);
    LOG.log(Level.INFO, blockId + " statistics on insert: " + statistics);
  }

  /**
   * Call before starting block loading.
   * Will wait for memory to free up if too much memory is being used for pin + load.
   * Updates statistics.
   * @param blockSize
   */
  public synchronized List<BlockId> loadStart(final BlockId blockId, final boolean pin) throws BlockNotFoundException, MemoryLimitException {
    LOG.log(Level.INFO, blockId+" statistics before loadStart: "+statistics);
    final long blockSize = blockId.getBlockSize();

    boolean canLoad = false;
    while (!canLoad) {
      // Check every iteration
      if (isState(blockId, CacheEntryState.REMOVED)) {
        if (pin) {
          statistics.subtractPinnedBytes(blockSize);
        } else {
          // nothing
        }
        throw new BlockNotFoundException(blockId+" was removed during INSERTED");
      }

      final long cached = statistics.getCacheBytes();
      if (cached < 0) {
        throw new RuntimeException(blockId+" cached is less than zero: "+cached);
      }

      final long usableCache = cacheSize - statistics.getPinnedBytes();
      final long freeCache = usableCache - statistics.getCacheBytes() - statistics.getLoadingBytes();
      LOG.log(Level.INFO, blockId+" size: "+blockSize+" free cache: "+freeCache+" usable cache: "+usableCache);
      if (blockSize > usableCache) {
        throw new MemoryLimitException(blockId+" ran out of usable cache: "+usableCache);
      } else if (blockSize > freeCache) {
        final List<BlockId> toEvict = lru.evict(blockSize - freeCache);
        if (toEvict.size() > 0) {
          return toEvict;
        }
      }

      canLoad = (blockSize <= freeCache);
      LOG.log(Level.INFO, blockId+" statistics during loadStart: "+statistics);

      if (!canLoad) {
        LOG.log(Level.WARNING, "Waiting to load block, "+statistics);
        try {
          wait();
        } catch (InterruptedException e) {
          LOG.log(Level.WARNING, "Wait interrupted", e);
        }
      }
    }

    if (pin) {
      // nothing
    } else {
      statistics.addLoadingBytes(blockSize);
    }
    setState(blockId, CacheEntryState.LOAD_STARTED);
    LOG.log(Level.INFO, blockId+" statistics after loadStart: "+statistics);
    return null; // No need to evict, can start loading
  }

  public synchronized void loadStartFail(final BlockId blockId, final boolean pin, final Exception exception) {
    LOG.log(Level.INFO, blockId+" statistics before loadStartFail: "+statistics);
    final long blockSize = blockId.getBlockSize();
    if (statistics.getCacheBytes() < 0) {
      throw new RuntimeException(blockId+" cached is less than zero");
    }

    if (pin) {
      statistics.subtractPinnedBytes(blockSize);
    } else {
      // nothing
    }
    updates.addFailure(blockId, exception);
    setState(blockId, CacheEntryState.REMOVED);
    notifyAll();
  }

  /**
   * Call on load success.
   * Notifies threads waiting for memory to free up.
   * Updates statistics.
   * @param blockSize
   * @param pin
   */
  public synchronized void loadSuccess(final BlockId blockId, final boolean pin) {
    LOG.log(Level.INFO, blockId+" statistics before loadSuccess: "+statistics);
    if (statistics.getCacheBytes() < 0) {
      throw new RuntimeException(blockId+" cached is less than zero");
    }

    final long blockSize = blockId.getBlockSize();
    final CacheEntryState state = getState(blockId);
    switch(state) {
      case LOAD_STARTED:
        if (pin) {
          // nothing
        } else {
          statistics.subtractLoadingBytes(blockSize);
          statistics.addCacheBytes(blockSize);
        }
        setState(blockId, CacheEntryState.LOAD_SUCCEEDED);
        notifyAll();
        break;
      case REMOVED_DURING_LOAD:
        if (pin) {
          statistics.subtractPinnedBytes(blockSize);
        } else {
          statistics.subtractLoadingBytes(blockSize);
        }
        lru.evicted(blockId);
        statistics.addEvictedBytes(blockSize);
        updates.addRemoval(blockId);
        setState(blockId, CacheEntryState.REMOVED);
        notifyAll();
        break;
      default:
        throw new RuntimeException(blockId+" unexpected state on loadSuccess "+getState(blockId));
    }

    LOG.log(Level.INFO, blockId + " statistics after loadSuccess: " + statistics);
  }

  /**
   * Call on load failure.
   * Notifies threads waiting for memory to free up.
   * Updates statistics.
   * @param blockSize
   */
  public synchronized void loadFail(final BlockId blockId, final boolean pinned, final Throwable throwable) {
    LOG.log(Level.INFO, blockId+" statistics before loadFail: "+statistics);
    if (statistics.getCacheBytes() < 0) {
      throw new RuntimeException(blockId+" cached is less than zero");
    }

    final long blockSize = blockId.getBlockSize();
    final CacheEntryState state = getState(blockId);
    switch(state) {
      case LOAD_STARTED:
        if (pinned) {
          statistics.subtractPinnedBytes(blockSize);
        } else {
          statistics.subtractLoadingBytes(blockSize);
        }
        updates.addFailure(blockId, throwable);
        setState(blockId, CacheEntryState.LOAD_FAILED);
        notifyAll();
        break;
      case REMOVED_DURING_LOAD:
        if (pinned) {
          statistics.subtractPinnedBytes(blockSize);
        } else {
          statistics.subtractLoadingBytes(blockSize);
        }
        lru.evicted(blockId);
        statistics.addEvictedBytes(blockSize);
        updates.addFailure(blockId, throwable);
        setState(blockId, CacheEntryState.REMOVED);
        notifyAll();
        break;
      default:
        throw new RuntimeException(blockId+" unexpected state on loadFail "+getState(blockId));
    }

    LOG.log(Level.INFO, blockId + " statistics after loadFail: " + statistics);
  }

  /**
   * Call on cache removal (eviction).
   * Notifies threads waiting for memory to free up.
   * Updates statistics.
   * @param blockId
   */
  public synchronized void remove(final BlockId blockId, final boolean pinned) {
    LOG.log(Level.INFO, blockId+" statistics before remove: "+statistics);
    if (statistics.getCacheBytes() < 0) {
      throw new RuntimeException(blockId+" cached is less than zero");
    }

    final long blockSize = blockId.getBlockSize();
    final CacheEntryState state = getState(blockId);
    switch(state) {
      case INSERTED:
        if (pinned) {
          statistics.subtractPinnedBytes(blockSize);
        }
        lru.evicted(blockId);
        statistics.addEvictedBytes(blockSize);
        updates.addRemoval(blockId);
        setState(blockId, CacheEntryState.REMOVED);
        break;
      case LOAD_STARTED:
        setState(blockId, CacheEntryState.REMOVED_DURING_LOAD);
        break;
      case LOAD_FAILED:
        lru.evicted(blockId);
        statistics.addEvictedBytes(blockSize);
        // don't update as removed, already updated as failed
        setState(blockId, CacheEntryState.REMOVED);
        break;
      case LOAD_SUCCEEDED:
        if (pinned) {
          statistics.subtractPinnedBytes(blockSize);
        } else {
          statistics.subtractCacheBytes(blockSize);
        }
        lru.evicted(blockId);
        statistics.addEvictedBytes(blockSize);
        updates.addRemoval(blockId);
        setState(blockId, CacheEntryState.REMOVED);
        notifyAll();
        break;
      default:
        throw new RuntimeException(blockId+" unexpected state on remove: "+state);
    }

    LOG.log(Level.INFO, blockId + " statistics after remove: " + statistics);
  }

  private void setState(final BlockId blockId, final CacheEntryState state) {
    LOG.log(Level.INFO, blockId+" set state "+state);

    if (CacheEntryState.REMOVED.equals(state)) {
      cacheEntries.remove(blockId);
    } else {
      cacheEntries.put(blockId, state);
    }
  }

  private CacheEntryState getState(final BlockId blockId) {
    if (cacheEntries.containsKey(blockId)) {
      return cacheEntries.get(blockId);
    } else {
      return CacheEntryState.REMOVED;
    }
  }

  private boolean isState(final BlockId blockId, final CacheEntryState state) {
    final CacheEntryState currentState = getState(blockId);
    return currentState.equals(state);
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

  public long getCacheSize() {
    return cacheSize;
  }
}
