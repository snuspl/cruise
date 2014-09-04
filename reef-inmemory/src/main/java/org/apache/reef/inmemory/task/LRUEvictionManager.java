package org.apache.reef.inmemory.task;

import com.google.common.cache.Cache;
import com.google.common.collect.Iterables;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class LRUEvictionManager {
  private static final Logger LOG = Logger.getLogger(LRUEvictionManager.class.getName());

  private final Map<BlockId, LRUEntry> index = new HashMap<>();

  // enter() -> tail -> next -> .... -> head -> exit()
  private LRUEntry head;
  private LRUEntry tail;

  private final class LRUEntry {
    public LRUEntry prev;
    public LRUEntry next;
    public final BlockId blockId;

    public LRUEntry(final BlockId blockId) {
      this.blockId = blockId;
    }

    private void remove() {
      if (this == tail && this == head) {
        tail = null;
        head = null;
      } else if (this == tail) {
        next.prev = null;
        tail = next;
      } else if (this == head) {
        prev.next = null;
        head = prev;
      } else {
        prev.next = next;
        next.prev = prev;
      }
    }

    public void use() {
      this.remove();
      this.enter();
    }

    public void enter() {
      if (tail == null) {
        tail = this;
        head = this;
      } else {
        next = tail;
        next.prev = this;
        tail = this;
      }
    }

    public void exit() {
      this.remove();
    }
  }

  /**
   * Accesses synchronized by MemoryManager
   */
  private long evictingBytes = 0;

  @Inject
  public LRUEvictionManager() {
  }

  public void add(final BlockId blockId) {
    final LRUEntry entry = new LRUEntry(blockId);
    index.put(blockId, entry);
    entry.enter();
  }

  public void use(final BlockId blockId) {
    final LRUEntry entry = index.get(blockId);
    if (entry != null) {
      entry.use();
    }
  }

  public List<BlockId> evict(final long spaceNeeded) {
    final long evictionNeeded = spaceNeeded - evictingBytes;
    LOG.log(Level.INFO, evictionNeeded+" to be evicted");

    long chosenSize = 0;
    final List<BlockId> chosen = new LinkedList<>();
    LRUEntry entry = head;
    while (entry != null && chosenSize < evictionNeeded) {
      chosen.add(entry.blockId);
      chosenSize += entry.blockId.getBlockSize();
      entry = entry.prev;
    }
    if (chosenSize >= evictionNeeded) {
      for (final BlockId blockToEvict : chosen) {
        final LRUEntry entryToEvict = index.remove(blockToEvict);
        entryToEvict.exit();
        addEvictingBytes(blockToEvict.getBlockSize());
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
