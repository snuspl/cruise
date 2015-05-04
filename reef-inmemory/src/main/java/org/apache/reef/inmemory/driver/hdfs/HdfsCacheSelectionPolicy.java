package org.apache.reef.inmemory.driver.hdfs;

import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.reef.inmemory.driver.CacheNode;

import java.util.List;
import java.util.Map;

/**
 * Policy for choosing CacheNodes for replicated caching, based on the block
 * info (including locations) provided by HDFS.
 */
public interface HdfsCacheSelectionPolicy {
  /**
   * For each block, return the CacheNodes to place replicas on.
   * Note, the CacheNodes list may be modified in place.
   */
  public Map<LocatedBlock, List<CacheNode>> select(final List<LocatedBlock> blocks,
                                                   final List<CacheNode> nodes,
                                                   final int numReplicas);
}
