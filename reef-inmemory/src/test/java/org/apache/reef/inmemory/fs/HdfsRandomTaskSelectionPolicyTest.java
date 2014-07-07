package org.apache.reef.inmemory.fs;

import com.microsoft.reef.driver.task.RunningTask;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Test simple random policy
 */
public final class HdfsRandomTaskSelectionPolicyTest {

  private LocatedBlock getMockBlock() {
    return mock(LocatedBlock.class);
  }

  private List<CacheNode> getMockCacheNodes(int size) {
    ArrayList<CacheNode> mockNodes = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      mockNodes.add(mock(CacheNode.class));
    }
    return mockNodes;
  }

  @Test
  public void testLessThanDefault() {
    HdfsRandomCacheSelectionPolicy policy = new HdfsRandomCacheSelectionPolicy(3);

    List<CacheNode> nodes = getMockCacheNodes(1);
    List<CacheNode> selected = policy.select(getMockBlock(), nodes);
    assertTrue(nodes.containsAll(selected));
    assertEquals(1, selected.size());
  }

  @Test
  public void testIsShuffled() {
    HdfsRandomCacheSelectionPolicy policy = new HdfsRandomCacheSelectionPolicy(3);
    List<CacheNode> nodes = getMockCacheNodes(100);
    for (int i = 0; i < 10; i++) { // With high probability
      List<CacheNode> selected = policy.select(getMockBlock(), new ArrayList<>(nodes));
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
