package org.apache.reef.inmemory.driver.hdfs;

import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.reef.inmemory.driver.CacheNode;

import javax.inject.Inject;
import java.util.*;

/**
 * Simple random policy for Cache selection
 */
public final class HdfsRandomCacheSelectionPolicy implements HdfsCacheSelectionPolicy {

  @Inject
  public HdfsRandomCacheSelectionPolicy() {
  }

  /**
   * Return random nodes. The number selected is min of numReplicas and nodes.size()
   */
  private List<CacheNode> select(final LocatedBlock block,
                                 final List<CacheNode> nodes,
                                 final int numReplicas) {
    Collections.shuffle(nodes);

    final List<CacheNode> chosenNodes = new ArrayList<>(numReplicas);
    int replicasAdded = 0;
    for (final CacheNode node : nodes) {
      if (replicasAdded >= numReplicas) break;
      chosenNodes.add(node);
      replicasAdded++;
    }
    return chosenNodes;
  }

  /**
   * Return map of random nodes. The number of nodes selected per block is min of numReplicas and nodes.size()
   */
  @Override
  public Map<LocatedBlock, List<CacheNode>> select(final List<LocatedBlock> blocks,
                                                   final List<CacheNode> nodes,
                                                   final int numReplicas) {
    final Map<LocatedBlock, List<CacheNode>> selected = new HashMap<>();
    for (final LocatedBlock locatedBlock : blocks) {
      selected.put(locatedBlock, select(locatedBlock, nodes, numReplicas));
    }
    return selected;
  }
}
