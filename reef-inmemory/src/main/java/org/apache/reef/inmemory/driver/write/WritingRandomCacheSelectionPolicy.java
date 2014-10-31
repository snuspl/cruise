package org.apache.reef.inmemory.driver.write;

import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.driver.CacheNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Choose just one cache node to write data into.
 */
public class WritingRandomCacheSelectionPolicy implements WritingCacheSelectionPolicy {
  @Override
  public List<NodeInfo> select(List<CacheNode> nodes, int numReplicas) {
    Collections.shuffle(nodes);
    final List<NodeInfo> chosenNodes = new ArrayList<>(numReplicas);
    int replicasAdded = 0;
    for (final CacheNode node : nodes) {
      if (replicasAdded >= numReplicas) break;
      chosenNodes.add(new NodeInfo(node.getAddress(), node.getRack()));
      replicasAdded++;
    }
    return chosenNodes;
  }
}
