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

  private final int numReplicas;

  @Inject
  public HdfsRandomCacheSelectionPolicy(final @Parameter(MetaServerParameters.Replicas.class) int numReplicas) {
    if (numReplicas < 1) {
      throw new IllegalArgumentException("Must select at least one replica");
    }
    this.numReplicas = numReplicas;
  }

  /**
   * Return random blocks. The number selected is min of numReplicas and tasks.size()
   */
  @Override
  public List<CacheNode> select(final LocatedBlock block,
                                final List<CacheNode> tasks) {
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
