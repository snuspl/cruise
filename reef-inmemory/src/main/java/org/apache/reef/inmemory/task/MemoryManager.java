package org.apache.reef.inmemory.task;

import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.CacheStatistics;
import org.apache.reef.inmemory.common.CacheUpdates;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;
import org.apache.reef.inmemory.common.exceptions.MemoryLimitException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Memory manager keeps track of memory statistics and applies admission control
 * when there is too much memory pressure.
 * Memory usage is calculated as the amount of copying + pinned memory (memory that cannot be GC'd)
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
    COPY_STARTED,
    COPY_SUCCEEDED,
    COPY_FAILED,
    REMOVED_DURING_COPY,
    REMOVED
  }

  /**
   * Call during cache insert call.
   */
  public synchronized void cacheInsert(final BlockId blockId, final long blockSize, final boolean pin) {
    if (isState(blockId, CacheEntryState.INSERTED)) {
      throw new RuntimeException(blockId+" has been previously inserted");
    }

    final long cached = statistics.getCacheBytes();
    if (cached < 0) {
      throw new RuntimeException(blockId+" cached is less than zero: "+cached);
    }

    if (pin) {
      statistics.addPinnedBytes(blockSize);
      lru.addPinned(blockId);
    } else {
      lru.add(blockId, blockSize);
    }

    setState(blockId, CacheEntryState.INSERTED);
    LOG.log(Level.INFO, blockId + " statistics on insert: " + statistics);
  }

  /**
   * Call before starting block copying.
   * Will wait for memory to free up if too much memory is being used for pin + copy.
   * Updates statistics.
   */
  public synchronized List<BlockId> copyStart(final BlockId blockId, final long blockSize, final boolean pin) throws BlockNotFoundException, MemoryLimitException {
    LOG.log(Level.INFO, blockId+" statistics before copyStart: "+statistics);

    boolean canCopy = false;
    while (!canCopy) {
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
      final long freeCache = usableCache - statistics.getCacheBytes() - statistics.getCopyingBytes();
      LOG.log(Level.INFO, blockId+" size: "+blockSize+" free cache: "+freeCache+" usable cache: "+usableCache);
      if (blockSize > usableCache) {
        throw new MemoryLimitException(blockId+" ran out of usable cache: "+usableCache);
      } else if (blockSize > freeCache) {
        final List<BlockId> toEvict = lru.evict(blockSize - freeCache);
        if (toEvict.size() > 0) {
          return toEvict;
        }
      }

      canCopy = (blockSize <= freeCache);
      LOG.log(Level.INFO, blockId+" statistics during copyStart: "+statistics);

      if (!canCopy) {
        LOG.log(Level.WARNING, "Waiting to copy block, "+statistics);
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
      statistics.addCopyingBytes(blockSize);
    }
    setState(blockId, CacheEntryState.COPY_STARTED);
    LOG.log(Level.INFO, blockId + " statistics after copyStart: " + statistics);
    return null; // No need to evict, can start copying
  }

  /**
   * Call when copyStart failure.
   */
  public synchronized void copyStartFail(final BlockId blockId, final long blockSize, final boolean pin, final Exception exception) {
    LOG.log(Level.INFO, blockId+" statistics before copyStartFail: "+statistics);
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
   * Call on load success. Notifies threads waiting for memory to free up and updates statistics.
   */
  public void loadSuccess(final BlockId id, final long blockSize, final boolean pin) {
    copySuccess(id, blockSize, pin);
  }

  /**
   * Call on write success. Notifies threads waiting for memory to free up, updates statistics,
   * and report the amount of written data.
   */
  public void writeSuccess(final BlockId id, final long blockSize, final boolean pin, final long nWritten) {
    final CacheEntryState state = copySuccess(id, blockSize, pin);
    if (state == CacheEntryState.COPY_SUCCEEDED) {
      updates.addAddition(id, nWritten);
    }
  }

  /**
   * Call on success of the copy.
   * @return The state of entry.
   */
  private synchronized CacheEntryState copySuccess(final BlockId blockId, final long blockSize, final boolean pin) {
    LOG.log(Level.INFO, blockId+" statistics before copySuccess: "+statistics);
    if (statistics.getCacheBytes() < 0) {
      throw new RuntimeException(blockId+" cached is less than zero");
    }

    final CacheEntryState originState = getState(blockId);
    final CacheEntryState targetState;
    switch(originState) {
      case COPY_STARTED:
        if (pin) {
          // nothing
        } else {
          statistics.subtractCopyingBytes(blockSize);
          statistics.addCacheBytes(blockSize);
        }
        targetState = CacheEntryState.COPY_SUCCEEDED;
        setState(blockId, targetState);
        notifyAll();
        break;
      case REMOVED_DURING_COPY:
        if (pin) {
          statistics.subtractPinnedBytes(blockSize);
        } else {
          statistics.subtractCopyingBytes(blockSize);
        }
        lru.evicted(blockSize);
        statistics.addEvictedBytes(blockSize);
        updates.addRemoval(blockId);
        targetState = CacheEntryState.REMOVED;
        setState(blockId, targetState);
        notifyAll();
        break;
      default:
        throw new RuntimeException(blockId+" unexpected state on copySuccess "+getState(blockId));
    }

    LOG.log(Level.INFO, blockId + " statistics after copySuccess: " + statistics);
    return targetState;
  }
  /**
   * Call on copy failure.
   * Notifies threads waiting for memory to free up.
   * Updates statistics.
   */
  public synchronized void copyFail(final BlockId blockId, final long blockSize, final boolean pinned, final Throwable throwable) {
    LOG.log(Level.INFO, blockId+" statistics before copyFail: "+statistics);
    if (statistics.getCacheBytes() < 0) {
      throw new RuntimeException(blockId+" cached is less than zero");
    }

    final CacheEntryState state = getState(blockId);
    switch(state) {
      case COPY_STARTED:
        if (pinned) {
          statistics.subtractPinnedBytes(blockSize);
        } else {
          statistics.subtractCopyingBytes(blockSize);
        }
        updates.addFailure(blockId, throwable);
        setState(blockId, CacheEntryState.COPY_FAILED);
        notifyAll();
        break;
      case REMOVED_DURING_COPY:
        if (pinned) {
          statistics.subtractPinnedBytes(blockSize);
        } else {
          statistics.subtractCopyingBytes(blockSize);
        }
        lru.evicted(blockSize);
        statistics.addEvictedBytes(blockSize);
        updates.addFailure(blockId, throwable);
        setState(blockId, CacheEntryState.REMOVED);
        notifyAll();
        break;
      default:
        throw new RuntimeException(blockId+" unexpected state on copyFail "+getState(blockId));
    }

    LOG.log(Level.INFO, blockId + " statistics after copyFail: " + statistics);
  }

  /**
   * Call on cache removal (eviction).
   * Notifies threads waiting for memory to free up.
   * Updates statistics.
   * @param blockId
   */
  public synchronized void remove(final BlockId blockId, final long blockSize, final boolean pinned) {
    LOG.log(Level.INFO, blockId+" statistics before remove: "+statistics);
    if (statistics.getCacheBytes() < 0) {
      throw new RuntimeException(blockId+" cached is less than zero");
    }

    final CacheEntryState state = getState(blockId);
    switch(state) {
      case INSERTED:
        if (pinned) {
          statistics.subtractPinnedBytes(blockSize);
        }
        lru.evicted(blockSize);
        statistics.addEvictedBytes(blockSize);
        updates.addRemoval(blockId);
        setState(blockId, CacheEntryState.REMOVED);
        break;
      case COPY_STARTED:
        setState(blockId, CacheEntryState.REMOVED_DURING_COPY);
        break;
      case COPY_FAILED:
        lru.evicted(blockSize);
        statistics.addEvictedBytes(blockSize);
        // don't update as removed, already updated as failed
        setState(blockId, CacheEntryState.REMOVED);
        break;
      case COPY_SUCCEEDED:
        if (pinned) {
          statistics.subtractPinnedBytes(blockSize);
        } else {
          statistics.subtractCacheBytes(blockSize);
        }
        lru.evicted(blockSize);
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
