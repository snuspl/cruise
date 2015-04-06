package org.apache.reef.inmemory.driver.write;

import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.driver.CacheNode;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Choose cache nodes to write by random
 * TODO Consider the locality with regard to the clientAddress
 */
public class WritingRandomCacheSelectionPolicy implements WritingCacheSelectionPolicy {
  @Inject
  WritingRandomCacheSelectionPolicy() {
  }

  @Override
  public List<NodeInfo> select(final List<CacheNode> nodes, final int numReplicas) {
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
