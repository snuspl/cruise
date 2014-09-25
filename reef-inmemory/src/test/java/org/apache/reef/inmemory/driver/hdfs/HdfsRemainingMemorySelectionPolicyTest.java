package org.apache.reef.inmemory.driver.hdfs;

import com.google.common.collect.Lists;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.reef.inmemory.common.CacheStatistics;
import org.apache.reef.inmemory.driver.CacheNode;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test simple random policy
 */
public final class HdfsRemainingMemorySelectionPolicyTest {

  private static final Random random = new Random();
  private static int nodeId = 0;
  private static int numReplicas = 3;

  private LocatedBlocks getMockBlocks(final LocatedBlock block) {
    final LocatedBlocks locatedBlocks = mock(LocatedBlocks.class);
    when(locatedBlocks.getLocatedBlocks()).thenReturn(Lists.newArrayList(block));
    return locatedBlocks;
  }

  private LocatedBlocks getMockBlocks(final List<LocatedBlock> block) {
    final LocatedBlocks locatedBlocks = mock(LocatedBlocks.class);
    when(locatedBlocks.getLocatedBlocks()).thenReturn(block);
    return locatedBlocks;
  }

  private LocatedBlock getMockBlock() {
    final LocatedBlock locatedBlock = mock(LocatedBlock.class);
    when(locatedBlock.getBlockSize()).thenReturn(128L * 1024L * 1024L);
    return locatedBlock;
  }

  private CacheNode getRandomCacheNode() {
    CacheStatistics statistics = new CacheStatistics((random.nextInt(8095)+1) * 1024L * 1024L);
    statistics.addCacheBytes(random.nextInt(8095) + 1);
    statistics.addLoadingBytes(random.nextInt(8095) + 1);

    CacheNode cacheNode = mock(CacheNode.class);
    when(cacheNode.getLatestStatistics()).thenReturn(statistics);
    when(cacheNode.getTaskId()).thenReturn(Integer.toString(++nodeId));
    return cacheNode;
  }

  private CacheNode getFreshCacheNode() {
    CacheStatistics statistics = new CacheStatistics(8096 * 1024L * 1024L);
    statistics.addCacheBytes(0);
    statistics.addLoadingBytes(0);

    CacheNode cacheNode = mock(CacheNode.class);
    when(cacheNode.getLatestStatistics()).thenReturn(statistics);
    when(cacheNode.getTaskId()).thenReturn(Integer.toString(++nodeId));
    return cacheNode;
  }


  private long getRemaining(CacheNode node) {
    return node.getLatestStatistics().getMaxBytes() -
            (node.getLatestStatistics().getCacheBytes() + node.getLatestStatistics().getLoadingBytes());
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
    final HdfsRemainingMemorySelectionPolicy policy = new HdfsRemainingMemorySelectionPolicy();

    final List<CacheNode> nodes = getFreshCacheNodes(10);
    final LocatedBlock block = getMockBlock();
    final LocatedBlocks blocks = getMockBlocks(block);
    final Map<LocatedBlock, List<CacheNode>> selected = policy.select(blocks.getLocatedBlocks(), new ArrayList<>(nodes), numReplicas);
    final List<CacheNode> selectedList = selected.get(block);
    assertTrue(nodes.containsAll(selectedList));
    assertEquals(numReplicas, selectedList.size());
  }

  /**
   * Test nodes selected when num nodes < num replicas
   */
  @Test
  public void testLessThanDefault() {
    final HdfsRemainingMemorySelectionPolicy policy = new HdfsRemainingMemorySelectionPolicy();

    final List<CacheNode> nodes = getRandomCacheNodes(2);
    final LocatedBlock block = getMockBlock();
    final LocatedBlocks blocks = getMockBlocks(block);
    final Map<LocatedBlock, List<CacheNode>> selected = policy.select(blocks.getLocatedBlocks(), new ArrayList<>(nodes), numReplicas);
    final List<CacheNode> selectedList = selected.get(block);
    assertTrue(nodes.containsAll(selectedList));
    assertEquals(2, selectedList.size());

    final long selectedRemaining = getRemaining(selectedList.get(0));
    for (final CacheNode node : nodes) {
      if (!selectedList.contains(node)) {
        assertTrue(selectedRemaining >= getRemaining(node));
      }
    }
  }

  /**
   * Test nodes in selected are indeed the largest
   */
  @Test
  public void testLargestSelected() {
    final HdfsRemainingMemorySelectionPolicy policy = new HdfsRemainingMemorySelectionPolicy();

    final List<CacheNode> nodes = getRandomCacheNodes(100);
    final LocatedBlock block = getMockBlock();
    final LocatedBlocks blocks = getMockBlocks(block);
    final Map<LocatedBlock, List<CacheNode>> selected = policy.select(blocks.getLocatedBlocks(), new ArrayList<>(nodes), numReplicas);
    final List<CacheNode> selectedList = selected.get(block);
    assertTrue(nodes.containsAll(selectedList));
    assertEquals(numReplicas, selectedList.size());

    for (final CacheNode sel : selectedList) {
      final long selectedRemaining = getRemaining(sel);
      for (final CacheNode node : nodes) {
        if (!selectedList.contains(node)) {
          assertTrue(selectedRemaining+", "+getRemaining(node), selectedRemaining >= getRemaining(node));
        }
      }
    }
  }

  /**
   * Test that a file with multiple blocks is evenly distributed
   */
  @Test
  public void testMultipleBlocks() {
    final HdfsRemainingMemorySelectionPolicy policy = new HdfsRemainingMemorySelectionPolicy();

    final List<CacheNode> nodes = new ArrayList<>(10);
    for (int i = 0; i < 10; i++) {
      nodes.add(getFreshCacheNode());
    }

    final List<LocatedBlock> blockList = new ArrayList<>(20);
    for (int i = 0; i < 20; i ++) {
      blockList.add(getMockBlock());
    }
    final LocatedBlocks blocks = getMockBlocks(blockList);
    final Map<LocatedBlock, List<CacheNode>> selected = policy.select(blocks.getLocatedBlocks(), new ArrayList<>(nodes), numReplicas);

    assertEquals(20, selected.size());

    final Map<CacheNode, Integer> counts = new HashMap<>();
    for (final LocatedBlock block : selected.keySet()) {
      for (final CacheNode node : selected.get(block)) {
        if (!counts.containsKey(node)) {
          counts.put(node, 0);
        }
        counts.put(node, counts.get(node)+1);
      }
    }
    for (final CacheNode node : counts.keySet()) {
      assertEquals("CacheNode "+node, Integer.valueOf(2 * numReplicas), counts.get(node));
    }
  }
}
