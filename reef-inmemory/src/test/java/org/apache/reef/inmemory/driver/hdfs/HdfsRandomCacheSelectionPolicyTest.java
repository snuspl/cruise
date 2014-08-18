package org.apache.reef.inmemory.driver.hdfs;

import com.google.common.collect.Lists;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.reef.inmemory.driver.CacheNode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test simple random policy
 */
public final class HdfsRandomCacheSelectionPolicyTest {

  private static final int numReplicas = 3;

  private LocatedBlocks getMockBlocks(final LocatedBlock block) {
    final LocatedBlocks locatedBlocks = mock(LocatedBlocks.class);
    when(locatedBlocks.getLocatedBlocks()).thenReturn(Lists.newArrayList(block));
    return locatedBlocks;
  }

  private LocatedBlock getMockBlock() {
    return mock(LocatedBlock.class);
  }

  private List<CacheNode> getMockCacheNodes(int size) {
    final ArrayList<CacheNode> mockNodes = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      mockNodes.add(mock(CacheNode.class));
    }
    return mockNodes;
  }

  @Test
  public void testLessThanDefault() {
    final HdfsRandomCacheSelectionPolicy policy = new HdfsRandomCacheSelectionPolicy();
    final LocatedBlock block = getMockBlock();
    final LocatedBlocks blocks = getMockBlocks(block);

    final List<CacheNode> nodes = getMockCacheNodes(1);
    final Map<LocatedBlock, List<CacheNode>> selected =
            policy.select(blocks, new ArrayList<>(nodes), numReplicas);
    assertNotNull(selected.get(block));
    assertTrue(nodes.containsAll(selected.get(block)));
    assertEquals(1, selected.size());
  }

  @Test
  public void testIsShuffled() {
    final HdfsRandomCacheSelectionPolicy policy = new HdfsRandomCacheSelectionPolicy();
    final LocatedBlock block = getMockBlock();
    final LocatedBlocks blocks = getMockBlocks(block);

    final List<CacheNode> nodes = getMockCacheNodes(100);
    for (int i = 0; i < 10; i++) { // With high probability
      final Map<LocatedBlock, List<CacheNode>> selected =
              policy.select(blocks, new ArrayList<>(nodes), numReplicas);
      assertTrue(nodes.containsAll(selected.get(block)));
      for (int j = 0; j < 3; j++) {
        if (nodes.get(j) != selected.get(j)) {
          return; // selected was not in the same order as nodes
        }
      }
    }
    assertTrue("Selected nodes do not appear to be shuffled", false);
  }
}
