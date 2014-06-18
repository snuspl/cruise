package org.apache.reef.inmemory.fs;

import com.microsoft.reef.driver.task.RunningTask;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import java.util.Collection;
import java.util.List;

/**
 * Policy for choosing Tasks for replicated caching, based on the block
 * info (including locations) provided by HDFS.
 */
public interface HdfsTaskSelectionPolicy {
  /**
   *
   * The caller should guarantee that tasks do not change
   * (e.g., called within a synchronized block).
   */
  public List<RunningTask> select(final LocatedBlock block,
                                  final Collection<RunningTask> tasks);
}
