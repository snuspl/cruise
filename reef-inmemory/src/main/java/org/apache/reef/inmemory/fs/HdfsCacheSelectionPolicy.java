package org.apache.reef.inmemory.fs;

import com.microsoft.reef.driver.task.RunningTask;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import java.util.Collection;
import java.util.List;

/**
 * Policy for choosing Tasks for replicated caching, based on the block
 * info (including locations) provided by HDFS.
 */
public interface HdfsCacheSelectionPolicy {
  /**
   * Return the Tasks to place cache replicas on.
   * Note, the Task list may be modified in place.
   */
  public List<CacheNode> select(final LocatedBlock block,
                                final List<CacheNode> tasks);
}
