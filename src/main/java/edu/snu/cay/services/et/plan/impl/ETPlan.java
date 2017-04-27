/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.services.et.plan.impl;

import edu.snu.cay.services.et.plan.api.Op;
import edu.snu.cay.services.et.plan.api.PlanExecutor;
import edu.snu.cay.utils.DAG;

import java.util.*;

/**
 * A plan to be executed by {@link PlanExecutor}.
 * Plan is a collection of ET operations ({@link Op}) and has a dependency information between them.
 * {@link PlanExecutor} can execute the plan by following steps.
 *   1. At first, call {@link #getInitialOps()} to obtain operations to execute.
 *   2. When the operation is completed, call {@link #onComplete(Op)} to mark it as completed
 *    and obtain a set of operations enabled by the completion of the operation.
 *   2-1. Start executing the obtained operations.
 *   2-2. If step 2 returns an empty set, check whether the whole plan is completed,
 *    using {@link #getNumTotalOps()}.
 *   3. Wait the completion of operations. Goto step 2 again.
 */
public final class ETPlan {
  private final DAG<Op> dependencyGraph;
  private final Set<Op> initialOps;
  private final int numTotalOps;

  /**
   * Creates a plan with dependency information between operations.
   */
  public ETPlan(final DAG<Op> dependencyGraph,
                final int numTotalOps) {
    this.dependencyGraph = dependencyGraph;
    this.initialOps = new HashSet<>(dependencyGraph.getRootVertices());

    // count the total number of operations
    this.numTotalOps = numTotalOps;
  }

  /**
   * Gets the total number of operations that compose the plan.
   * @return the number of total number of operations
   */
  public int getNumTotalOps() {
    return numTotalOps;
  }

  /**
   * Gets initial operations to start with.
   * @return a set of initial operations
   */
  public synchronized Set<Op> getInitialOps() {
    return initialOps;
  }

  /**
   * Marks the operation complete.
   * It returns operations that become ready, which means that they have no prerequisite operations,
   * at the completion of the operation.
   * @param completedOp the completed operation
   * @return a set of operations that become ready
   */
  public synchronized Set<Op> onComplete(final Op completedOp) {
    final Set<Op> candidateOps = dependencyGraph.getNeighbors(completedOp);
    final Set<Op> nextOpsToExecute = new HashSet<>();
    dependencyGraph.removeVertex(completedOp);

    for (final Op candidate : candidateOps) {
      if (dependencyGraph.getInDegree(candidate) == 0) {
        nextOpsToExecute.add(candidate);
      }
    }
    return nextOpsToExecute;
  }
}
