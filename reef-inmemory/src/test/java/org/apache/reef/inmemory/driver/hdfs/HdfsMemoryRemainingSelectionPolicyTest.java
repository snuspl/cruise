package org.apache.reef.inmemory.driver.hdfs;

import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.reef.inmemory.common.CacheStatistics;
import org.apache.reef.inmemory.driver.CacheNode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test simple random policy
 */
public final class HdfsMemoryRemainingSelectionPolicyTest {

  private static final Random random = new Random();
  private static int nodeId = 0;
  private static int numReplicas = 3;

  private LocatedBlock getMockBlock() {
    return mock(LocatedBlock.class);
  }

  private CacheNode getRandomCacheNode() {
    CacheStatistics statistics = new CacheStatistics();
    statistics.addCacheMB(random.nextInt(8095)+1);
    statistics.addLoadingMB(random.nextInt(8095)+1);

    CacheNode cacheNode = mock(CacheNode.class);
    when(cacheNode.getLatestStatistics()).thenReturn(statistics);
    when(cacheNode.getTaskId()).thenReturn(Integer.toString(++nodeId));
    when(cacheNode.getMemory()).thenReturn(random.nextInt(8095)+1); // Stats may show more memory used than exists
    return cacheNode;
  }

  private CacheNode getFreshCacheNode() {
    CacheStatistics statistics = new CacheStatistics();
    statistics.addCacheMB(0);
    statistics.addLoadingMB(0);

    CacheNode cacheNode = mock(CacheNode.class);
    when(cacheNode.getLatestStatistics()).thenReturn(statistics);
    when(cacheNode.getTaskId()).thenReturn(Integer.toString(++nodeId));
    when(cacheNode.getMemory()).thenReturn(8096); // Stats may show more memory used than exists
    return cacheNode;
  }


  private int getRemaining(CacheNode node) {
    return node.getMemory() -
            (node.getLatestStatistics().getCacheMB() + node.getLatestStatistics().getLoadingMB());
  }

  private List<CacheNode> getRandomCacheNodes(int size) {
    final ArrayList<CacheNode> mockNodes = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      mockNodes.add(getRandomCacheNode());
    }
    return mockNodes;
  }

  private List<CacheNode> getFreshCacheNodes(int size) {
    final ArrayList<CacheNode> mockNodes = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      mockNodes.add(getFreshCacheNode());
    }
    return mockNodes;
  }

  /**
   * Test nodes selected when all nodes are tied
   */
  @Test
  public void testAllTied() {
    final HdfsMemoryRemainingSelectionPolicy policy = new HdfsMemoryRemainingSelectionPolicy();

    final List<CacheNode> nodes = getFreshCacheNodes(10);
    final List<CacheNode> selected = policy.select(getMockBlock(), new ArrayList<>(nodes), numReplicas);
    assertTrue(nodes.containsAll(selected));
    assertEquals(numReplicas, selected.size());
  }

  /**
   * Test nodes selected when num nodes < num replicas
   */
  @Test
  public void testLessThanDefault() {
    final HdfsMemoryRemainingSelectionPolicy policy = new HdfsMemoryRemainingSelectionPolicy();

    final List<CacheNode> nodes = getRandomCacheNodes(2);
    final List<CacheNode> selected = policy.select(getMockBlock(), new ArrayList<>(nodes), numReplicas);
    assertTrue(nodes.containsAll(selected));
    assertEquals(2, selected.size());

    final int selectedRemaining = getRemaining(selected.get(0));
    for (final CacheNode node : nodes) {
      if (!selected.contains(node)) {
        assertTrue(selectedRemaining >= getRemaining(node));
      }
    }
  }

  /**
   * Test nodes in selected are indeed the largest
   */
  @Test
  public void testLargestSelected() {
    final HdfsMemoryRemainingSelectionPolicy policy = new HdfsMemoryRemainingSelectionPolicy();
    final List<CacheNode> nodes = getRandomCacheNodes(100);

    final List<CacheNode> selected = policy.select(getMockBlock(), new ArrayList<>(nodes), numReplicas);
    assertTrue(nodes.containsAll(selected));
    assertEquals(numReplicas, selected.size());

    for (final CacheNode sel : selected) {
      final int selectedRemaining = getRemaining(sel);
      for (final CacheNode node : nodes) {
        if (!selected.contains(node)) {
          assertTrue(selectedRemaining >= getRemaining(node));
        }
      }
    }
  }
}
