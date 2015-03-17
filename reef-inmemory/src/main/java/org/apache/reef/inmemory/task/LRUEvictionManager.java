package org.apache.reef.inmemory.task;

import org.apache.reef.inmemory.common.BlockId;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An LRU eviction implementation.
 * On memory usage, the manager must be informed when
 * - A block has been inserted into the cache: add()
 * - A block has been read from the cache: use()
 * - A block has been removed from the cache: evicted()
 */
public final class LRUEvictionManager {
  private static final Logger LOG = Logger.getLogger(LRUEvictionManager.class.getName());

  private final Map<BlockId, LRUEntry> index = new HashMap<>();
  private final Set<BlockId> pinned = new HashSet<>();

  private LRUEntry head;
  private LRUEntry tail;

  private long evictingBytes = 0;

  @Inject
  public LRUEvictionManager() {
  }

  /**
   * Invoke when a block has been inserted into the cache
   * @param blockId Inserted block
   */
  public synchronized void add(final BlockId blockId) {
    final LRUEntry entry = new LRUEntry(blockId);
    index.put(blockId, entry);
    entry.enter();
  }

  /**
   * Invoke when a block has been read from the cache
   * @param blockId Read block
   */
  public synchronized void use(final BlockId blockId) {
    final LRUEntry entry = index.get(blockId);
    if (entry != null) {
      entry.use();
    }
  }

  /**
   * Invoke when a block has been removed from the cache
   */
  public synchronized void evicted(final BlockId blockId) {
    evictingBytes -= blockId.getBlockSize();
  }

  public synchronized void addPinned(final BlockId blockId) {
    pinned.add(blockId);
  }

  /**
   * Evict blocks.
   * A list of Block IDs is returned, and the caller _must_ remove these blocks from the cache.
   * Once returned, the same Block ID will not be considered for eviction again.
   * @param spaceNeeded Space that needs to be cleared by eviction
   * @return List of Block IDs that caller must remove from cache
   */
  public synchronized List<BlockId> evict(final long spaceNeeded) {
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
        evictingBytes += blockToEvict.getBlockSize();
      }
      return chosen;
    } else {
      throw new RuntimeException(evictionNeeded+" eviction was unsuccessful.");
    }
  }

  /**
   * Evict all blocks.
   * A list of Block IDs is returned, and the caller _must_ remove these blocks from the cache.
   * Once returned, the same Block ID will not be considered for eviction again.
   * @param evictPinned Set true to evict all pinned blocks as well
   * @return List of Block IDs that caller must remove from cache
   */
  public synchronized List<BlockId> evictAll(final boolean evictPinned) {
    final List<BlockId> chosen = new ArrayList<>(index.size());
    LRUEntry entry = head;
    while (entry != null) {
      chosen.add(entry.blockId);
      entry = entry.prev;
    }
    for (final BlockId blockToEvict : chosen) {
      final LRUEntry entryToEvict = index.remove(blockToEvict);
      entryToEvict.exit();
      evictingBytes += blockToEvict.getBlockSize();
    }

    if (evictPinned) {
      chosen.addAll(pinned);
      pinned.clear();
    }

    return chosen;
  }

  /**
   * A simple double-linked list for efficient insert, removal, update and
   * retrieval of the least recently used item.
   *
   * LRUEntry enters at the tail and exits at head:
   * enter() -> tail -> next -> .... -> head -> exit()
   * On a use(), the entry is moved back to the tail.
   */
  private final class LRUEntry {
    public LRUEntry prev = null;
    public LRUEntry next = null;
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
      next = null;
      prev = null;
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
}
