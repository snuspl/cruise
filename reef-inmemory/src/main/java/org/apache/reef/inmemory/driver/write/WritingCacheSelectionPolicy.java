package org.apache.reef.inmemory.driver.write;

import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.driver.CacheNode;

import java.util.List;

/**
 * Choose one of the cache nodes to write a block
 */
public interface WritingCacheSelectionPolicy {
  public List<NodeInfo> select(final List<CacheNode> nodes, final int numReplicas);
}
