package org.apache.reef.inmemory.driver.hdfs;

import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.reef.inmemory.driver.CacheNode;
import org.apache.reef.inmemory.driver.hdfs.HdfsRandomCacheSelectionPolicy;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Test simple random policy
 */
public final class HdfsRandomCacheSelectionPolicyTest {

  private static final int numReplicas = 3;

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

    final List<CacheNode> nodes = getMockCacheNodes(1);
    final List<CacheNode> selected = policy.select(getMockBlock(), new ArrayList<>(nodes), numReplicas);
    assertTrue(nodes.containsAll(selected));
    assertEquals(1, selected.size());
  }

  @Test
  public void testIsShuffled() {
    final HdfsRandomCacheSelectionPolicy policy = new HdfsRandomCacheSelectionPolicy();
    final List<CacheNode> nodes = getMockCacheNodes(100);
    for (int i = 0; i < 10; i++) { // With high probability
      final List<CacheNode> selected = policy.select(getMockBlock(), new ArrayList<>(nodes), numReplicas);
      assertTrue(nodes.containsAll(selected));
      for (int j = 0; j < 3; j++) {
        if (nodes.get(j) != selected.get(j)) {
          return; // selected was not in the same order as nodes
        }
      }
    }
    assertTrue("Selected nodes do not appear to be shuffled", false);
  }
}
