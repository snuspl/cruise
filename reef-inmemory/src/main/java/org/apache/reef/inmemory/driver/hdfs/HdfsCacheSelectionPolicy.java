package org.apache.reef.inmemory.driver.hdfs;

import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.reef.inmemory.driver.CacheNode;

import java.util.List;

/**
 * Policy for choosing Tasks for replicated caching, based on the block
 * info (including locations) provided by HDFS.
 */
public interface HdfsCacheSelectionPolicy {
  /**
   * Return the Tasks to place task replicas on.
   * Note, the Task list may be modified in place.
   */
  public List<CacheNode> select(final LocatedBlock block,
                                final List<CacheNode> tasks,
                                final int numReplicas);
}
