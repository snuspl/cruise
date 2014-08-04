package org.apache.reef.inmemory.driver.hdfs;

import com.microsoft.tang.annotations.Parameter;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.reef.inmemory.driver.CacheNode;
import org.apache.reef.inmemory.driver.service.MetaServerParameters;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Selects the CacheNodes with the most memory remaining
 */
public final class HdfsMemoryRemainingSelectionPolicy implements HdfsCacheSelectionPolicy {

  private static final Logger LOG = Logger.getLogger(HdfsMemoryRemainingSelectionPolicy.class.getName());

  private final RemainingComparator comparator = new RemainingComparator();

  @Inject
  public HdfsMemoryRemainingSelectionPolicy() {
  }

  @Override
  public List<CacheNode> select(final LocatedBlock block,
                                final List<CacheNode> nodes,
                                final int numReplicas) {
    final SortedSet<CacheNode> selected = new TreeSet<>(comparator);

    for (CacheNode node: nodes) {
      if (selected.size() < numReplicas || comparator.compare(node, selected.first()) > 0) {
        selected.add(node);
        if (selected.size() > numReplicas) {
          selected.remove(selected.first());
        }
      }
    }

    for (CacheNode sel : selected) {
      LOG.log(Level.INFO, "Selected: {0}", sel.getAddress());
    }

    return new ArrayList<>(selected);
  }

  /**
   * Compare the remaining memory. Ties are broken by taskId, because a return value of
   * 0 implies equals for SortedSet.
   *
   * If running this comparison every time is too slow, may need to memoize or otherwise use another strategy.
   */
  private static class RemainingComparator implements Comparator<CacheNode> {

    @Override
    public int compare(CacheNode n1, CacheNode n2) {
      final int used1 = n1.getLatestStatistics().getCacheMB() +
              n1.getLatestStatistics().getLoadingMB();
      final int remaining1 = n1.getMemory() - used1;

      final int used2 = n2.getLatestStatistics().getCacheMB() +
              n2.getLatestStatistics().getLoadingMB();
      final int remaining2 = n2.getMemory() - used2;

      if (remaining1 != remaining2) {
        return remaining1 - remaining2;
      } else {
        // Break ties using taskId
        return n1.getTaskId().compareTo(n2.getTaskId());
      }
    }
  }
}
