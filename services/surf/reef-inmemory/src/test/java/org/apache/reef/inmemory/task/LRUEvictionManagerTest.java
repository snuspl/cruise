package org.apache.reef.inmemory.task;

import org.apache.reef.inmemory.common.BlockId;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for LRUEvictionManager, checking whether the manager
 * maintains LRU order and evicts / reports errors correctly.
 */
public final class LRUEvictionManagerTest {
  private LRUEvictionManager lru;
  private final Random random = new Random();
  private final long blockSize = 10;

  private BlockId randomBlockId() {
    return new BlockId(random.nextLong(), random.nextLong());
  }

  @Before
  public void setUp() {
    lru = new LRUEvictionManager();
  }

  /**
   * Test that eviction happens in add order (with no uses)
   */
  @Test
  public void testAddOrder() {
    final BlockId[] blockIds = new BlockId[]{
            randomBlockId(),
            randomBlockId(),
            randomBlockId()
    };

    for (int i = 0; i < blockIds.length; i++) {
      System.out.println("Add: "+blockIds[i]);
      lru.add(blockIds[i], blockSize);
    }

    // Eviction should happen in FIFO order, with no use
    long spaceNeeded = 0;
    for (int i = 0; i < blockIds.length; i++) {
      spaceNeeded += blockSize;
      final List<BlockId> toEvict = lru.evict(spaceNeeded);
      assertEquals(1, toEvict.size());
      assertEquals(blockIds[i], toEvict.get(0));
    }
  }

  /**
   * Test that eviction happens in use order
   */
  @Test
  public void testSingleUseOrder() {
    final BlockId[] blockIds = new BlockId[]{
            randomBlockId(),
            randomBlockId(),
            randomBlockId()
    };

    for (int i = 0; i < blockIds.length; i++) {
      System.out.println("Add: "+blockIds[i]);
      lru.add(blockIds[i], blockSize);
    }

    lru.use(blockIds[1]);
    final int[] order = new int[]{0, 2, 1};

    // Eviction order should be 0, 2, 1
    long spaceNeeded = 0;
    for (int i = 0; i < blockIds.length; i++) {
      spaceNeeded += blockSize;
      final List<BlockId> toEvict = lru.evict(spaceNeeded);
      assertEquals(1, toEvict.size());
      assertEquals(blockIds[order[i]], toEvict.get(0));
    }
  }

  /**
   * Test an eviction that requires multiple blocks to execute
   */
  @Test
  public void testEvictAll() {
    final BlockId[] blockIds = new BlockId[]{
            randomBlockId(),
            randomBlockId(),
            randomBlockId()
    };

    for (int i = 0; i < blockIds.length; i++) {
      System.out.println("Add: "+blockIds[i]);
      lru.add(blockIds[i], blockSize);
    }

    final long spaceNeeded = blockSize * blockIds.length;
    final List<BlockId> toEvict = lru.evict(spaceNeeded);
    assertEquals(3, toEvict.size());
  }

  /**
   * Test a failed eviction, due to not enough memory.
   * When failed, the eviction should not remove any blocks.
   */
  @Test
  public void testEvictNotPossible() {
    final BlockId[] blockIds = new BlockId[]{
            randomBlockId(),
            randomBlockId(),
            randomBlockId()
    };

    for (int i = 0; i < blockIds.length; i++) {
      System.out.println("Add: "+blockIds[i]);
      lru.add(blockIds[i], blockSize);
    }

    try {
      final long spaceNeeded = blockSize * (blockIds.length + 1);
      final List<BlockId> toEvict = lru.evict(spaceNeeded);
      assertTrue(false); // expected exception
    } catch (RuntimeException e) {
      assertTrue(true); // expected exception
    }

    {
      final long spaceNeeded = blockSize * blockIds.length;
      final List<BlockId> toEvict = lru.evict(spaceNeeded);
      assertEquals(3, toEvict.size());
    }

    try {
      final long spaceNeeded = blockSize * (blockIds.length + 1);
      final List<BlockId> toEvict = lru.evict(spaceNeeded);
      assertTrue(false); // expected exception
    } catch (RuntimeException e) {
      assertTrue(true); // expected exception
    }
  }
}
