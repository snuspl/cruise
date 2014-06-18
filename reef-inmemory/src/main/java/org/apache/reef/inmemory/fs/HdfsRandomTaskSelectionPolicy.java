package org.apache.reef.inmemory.fs;

import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.tang.annotations.Parameter;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.reef.inmemory.fs.service.MetaServerParameters;

import javax.inject.Inject;
import java.util.*;

/**
 * Simple random policy for Task selection
 */
public final class HdfsRandomTaskSelectionPolicy implements HdfsTaskSelectionPolicy {

  private final int numReplicas;

  @Inject
  public HdfsRandomTaskSelectionPolicy(final @Parameter(MetaServerParameters.Replicas.class) int numReplicas) {
    if (numReplicas < 1) {
      throw new IllegalArgumentException("Must select at least one replica");
    }
    this.numReplicas = numReplicas;
  }

  @Override
  public List<RunningTask> select(final LocatedBlock block,
                                  final Collection<RunningTask> tasks) {
    List<RunningTask> orderedTasks = new LinkedList<>(tasks);
    Collections.shuffle(orderedTasks);

    final List<RunningTask> tasksToCache = new ArrayList<>(numReplicas);
    int replicasAdded = 0;
    for (RunningTask task : orderedTasks) {
      if (replicasAdded >= numReplicas) break;
      tasksToCache.add(task);
      replicasAdded++;
    }
    return tasksToCache;
  }
}
