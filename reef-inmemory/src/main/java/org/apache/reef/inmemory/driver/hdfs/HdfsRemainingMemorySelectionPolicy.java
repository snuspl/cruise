package org.apache.reef.inmemory.driver.hdfs;

import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.reef.inmemory.common.CacheStatistics;
import org.apache.reef.inmemory.driver.CacheNode;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Logger;

/**
 * Selects the CacheNodes with the most memory remaining
 */
public final class HdfsRemainingMemorySelectionPolicy implements HdfsCacheSelectionPolicy {

  private static final Logger LOG = Logger.getLogger(HdfsRemainingMemorySelectionPolicy.class.getName());

  @Inject
  public HdfsRemainingMemorySelectionPolicy() {
  }

  private List<CacheNode> select(final LocatedBlock block,
                                 final PriorityQueue<RemainingMemory> remainings,
                                 final int numReplicas) {
    // Select top remaining, and remove for update
    final List<RemainingMemory> selected = new ArrayList<>(numReplicas);
    for (int i = 0; i < numReplicas; i++) {
      if (remainings.size() > 0) {
        selected.add(remainings.poll());
      }
    }

    // Update remaining memory, insert back, and add to return list
    final List<CacheNode> selectedNodes = new ArrayList<>(selected.size());
    for (final RemainingMemory remaining : selected) {
      final long used = block.getBlockSize() + remaining.getUsed();
      remaining.setUsed(used);
      remainings.add(remaining);
      selectedNodes.add(remaining.getNode());
    }
    return selectedNodes;
  }

  @Override
  public Map<LocatedBlock, List<CacheNode>> select(final LocatedBlocks blocks,
                                                   final List<CacheNode> nodes,
                                                   final int numReplicas) {
    final Map<LocatedBlock, List<CacheNode>> selected = new HashMap<>();

    final PriorityQueue<RemainingMemory> remainings = new PriorityQueue<>();
    for (final CacheNode node : nodes) {
      remainings.add(new RemainingMemory(node));
    }

    for (final LocatedBlock block : blocks.getLocatedBlocks()) {
      selected.put(block, select(block, remainings, numReplicas));
    }

    return selected;
  }

  /**
   * Store and compare the remaining memory. Used to keep the prioirity queue in descending order.
   * Ties are broken by taskId.
   */
  private static class RemainingMemory implements Comparable<RemainingMemory> {

    private final CacheNode node;
    private final long max;
    private long used;

    private RemainingMemory(final CacheNode node) {
      this.node = node;
      final CacheStatistics statistics = node.getLatestStatistics();
      this.max = statistics.getMaxBytes();
      this.used = statistics.getCacheBytes() + statistics.getLoadingBytes();
    }

    public CacheNode getNode() {
      return node;
    }

    public long getUsed() {
      return used;
    }

    public void setUsed(long used) {
      this.used = used;
    }

    public long getRemaining() {
      return max - used;
    }

    /**
     * Sort memory remaining in descending order
     */
    @Override
    public int compareTo(RemainingMemory that) {
      final int comparison = Long.compare(that.getRemaining(), this.getRemaining());
      if (comparison == 0) {
        return that.getNode().getTaskId().compareTo(this.getNode().getTaskId());
      } else {
        return comparison;
      }
    }
  }
}
