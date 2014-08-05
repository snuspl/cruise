package org.apache.reef.inmemory.driver.hdfs;

import com.microsoft.tang.annotations.Parameter;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.reef.inmemory.driver.CacheNode;
import org.apache.reef.inmemory.driver.service.MetaServerParameters;

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
   * Return random nodes. The number selected is min of numReplicas and tasks.size()
   */
  @Override
  public List<CacheNode> select(final LocatedBlock block,
                                final List<CacheNode> tasks,
                                final int numReplicas) {
    Collections.shuffle(tasks);

    final List<CacheNode> chosenNodes = new ArrayList<>(numReplicas);
    int replicasAdded = 0;
    for (final CacheNode node : tasks) {
      if (replicasAdded >= numReplicas) break;
      chosenNodes.add(node);
      replicasAdded++;
    }
    return chosenNodes;
  }
}
