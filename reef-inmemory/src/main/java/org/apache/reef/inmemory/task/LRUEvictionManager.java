package org.apache.reef.inmemory.task;

import com.google.common.cache.Cache;
import com.google.common.collect.Iterables;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class LRUEvictionManager {
  private static final Logger LOG = Logger.getLogger(LRUEvictionManager.class.getName());
  private static final Integer ACCESS = Integer.valueOf(0);

  private final LinkedHashMap<BlockId, Integer> accessOrder = new LinkedHashMap<>(16, 0.75f, true); // TODO: default sizes for now

  /**
   * Accesses synchronized by MemoryManager
   */
  private long evictingBytes = 0;

  @Inject
  public LRUEvictionManager() {
  }

  public void add(final BlockId blockId) {
    accessOrder.put(blockId, ACCESS);
  }

  public void use(final BlockId blockId) {
    accessOrder.get(blockId);
  }

  public List<BlockId> evict(final long spaceNeeded) {
    final long evictionNeeded = spaceNeeded - evictingBytes;
    LOG.log(Level.INFO, evictionNeeded+" to be evicted");

    long chosenSize = 0;
    final List<BlockId> chosen = new LinkedList<>();
    final Iterator<BlockId> it = accessOrder.keySet().iterator();
    while (it.hasNext() && chosenSize < evictionNeeded) {
      final BlockId blockId = it.next();
      chosen.add(blockId);
      chosenSize += blockId.getBlockSize();
    }
    if (chosenSize >= evictionNeeded) {
      for (final BlockId blockId : chosen) {
        accessOrder.remove(blockId);
        addEvictingBytes(blockId.getBlockSize());
      }
      return chosen;
    } else {
      throw new RuntimeException(evictionNeeded+" eviction was unsuccessful.");
    }
  }

  public long getEvictingBytes() {
    return evictingBytes;
  }

  public void subtractEvictingBytes(long amount) {
    evictingBytes -= amount;
  }

  private void addEvictingBytes(long amount) {
    evictingBytes += amount;
  }
}
