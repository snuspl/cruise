package org.apache.reef.inmemory.task;

import com.google.common.cache.Cache;

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

  private final Cache<BlockId, BlockLoader> cache;
  private final LinkedHashMap<BlockId, Integer> accessOrder = new LinkedHashMap<>(16, 0.75f, true); // TODO: default sizes for now

  @Inject
  public LRUEvictionManager(final Cache<BlockId, BlockLoader> cache) {
    this.cache = cache;
  }

  public void add(final BlockId blockId) {
    accessOrder.put(blockId, ACCESS);
  }

  public void use(final BlockId blockId) {
    accessOrder.get(blockId);
  }

  public boolean evict(final long spaceNeeded) {
    LOG.log(Level.INFO, spaceNeeded+" to be evicted");

    long chosenSize = 0;
    final List<BlockId> chosen = new LinkedList<>();
    final Iterator<BlockId> it = accessOrder.keySet().iterator();
    while (it.hasNext() && chosenSize < spaceNeeded) {
      final BlockId blockId = it.next();
      chosen.add(blockId);
      chosenSize += blockId.getBlockSize();
    }
    if (chosenSize >= spaceNeeded) {
      for (final BlockId blockId : chosen) {
        accessOrder.remove(blockId);
        // Cache invalidations only take place here, and this is locked
        cache.invalidate(blockId);
      }
      return true;
    } else {
      LOG.log(Level.SEVERE, spaceNeeded+" eviction was not possible."); // TODO: throw an exception
      return false;
    }
  }
}
