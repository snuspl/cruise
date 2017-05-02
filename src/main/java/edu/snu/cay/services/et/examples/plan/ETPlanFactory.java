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
package edu.snu.cay.services.et.examples.plan;

import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.AllocatedTable;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import edu.snu.cay.services.et.plan.api.Op;
import edu.snu.cay.services.et.plan.impl.ETPlan;
import edu.snu.cay.services.et.plan.impl.op.*;
import edu.snu.cay.utils.DAG;
import edu.snu.cay.utils.DAGImpl;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A factory class that generates plans for testing {@link edu.snu.cay.services.et.plan.api.PlanExecutor}.
 */
final class ETPlanFactory {
  private static final String EXECUTOR_ID_PREFIX = "Executor-";

  private final ETMaster etMaster;

  @Inject
  private ETPlanFactory(final ETMaster etMaster) {
    this.etMaster = etMaster;
  }

  /**
   * Build an {@link ETPlan} that allocates executors with the same configuration.
   * This plan has no dependency, so all operations should be executed completely parallel.
   * @param numExecutors the number of executor to allocate
   * @param executorConf an executor configuration
   * @return a {@link ETPlan}
   */
  ETPlan getAllocateExecutorsPlan(final int numExecutors,
                                  final ExecutorConfiguration executorConf) {
    final DAG<Op> dag = new DAGImpl<>();
    final AtomicInteger opCounter = new AtomicInteger(0);

    for (int executorIdx = 0; executorIdx < numExecutors; executorIdx++) {
      final AllocateOp allocateOp = new AllocateOp(EXECUTOR_ID_PREFIX + executorIdx, executorConf);
      dag.addVertex(allocateOp);
      opCounter.getAndIncrement();
    }

    return new ETPlan(dag, opCounter.get());
  }

  /**
   * Build an {@link ETPlan} that deallocates executors.
   * This plan has no dependency, so all operations should be executed completely parallel.
   * @param executorIds a collection of executor ids to deallocate
   * @return a {@link ETPlan}
   */
  ETPlan getDeallocateExecutorsPlan(final Collection<String> executorIds) {
    final DAG<Op> dag = new DAGImpl<>();
    final AtomicInteger opCounter = new AtomicInteger(0);

    executorIds.forEach(executorId -> {
      final DeallocateOp deallocateOp = new DeallocateOp(executorId);
      dag.addVertex(deallocateOp);
      opCounter.getAndIncrement();
    });

    return new ETPlan(dag, opCounter.get());
  }

  /**
   * Build an {@link ETPlan} that moves all blocks to a target executor specified with {@code executorId},
   * so others will have no blocks.
   * @param srcExecutorIds a set of src executor ids
   * @param dstExecutorId a destination executor id
   * @param tableId an identifier of table.
   * @return a {@link ETPlan}
   * @throws TableNotExistException when a table with {@code tableId} does not exist
   */
  ETPlan getMoveAllBlocksToOneExecutorPlan(final Set<String> srcExecutorIds,
                                           final String dstExecutorId,
                                           final String tableId) throws TableNotExistException {
    final DAG<Op> dag = new DAGImpl<>();
    final AtomicInteger opCounter = new AtomicInteger(0);

    final AllocatedTable table = etMaster.getTable(tableId);
    srcExecutorIds.forEach(srcExecutorId -> {
      final int numBlocksToMove = table.getPartitionInfo().get(srcExecutorId).size();
      final MoveOp moveOp = new MoveOp(srcExecutorId, dstExecutorId, tableId, numBlocksToMove);
      dag.addVertex(moveOp);
      opCounter.getAndIncrement();
    });

    return new ETPlan(dag, opCounter.get());
  }

  /**
   * Build an {@link ETPlan} that has {@link MoveOp}s that form stage-dependency.
   * In this dependency, a set of moves happens in a stage.
   * In every stage, each executor moves {@code numBlocksToMove} blocks to a next executor.
   * So this plan does not change the number of blocks in each executor.
   * @param executors a list of executors
   * @param tableId an identifier of table
   * @return a {@link ETPlan}
   */
  ETPlan getMoveBlocksWithMultiStagesPlan(final List<AllocatedExecutor> executors,
                                          final String tableId) {
    final int numStages = 3;
    final int numBlocksToMove = 50;

    final DAG<Op> dag = new DAGImpl<>();
    final AtomicInteger opCounter = new AtomicInteger(0);

    final List<MoveOp> moveOpsInPrevStage = new ArrayList<>();
    final List<MoveOp> moveOpsInCurrStage = new ArrayList<>();

    for (int stage = 0; stage < numStages; stage++) {
      for (int index = 0; index < executors.size(); index++) {
        final String srcId = executors.get(index).getId();
        // move blocks to executor that has next index of executors.
        final String dstId = executors.get((index + 1) % executors.size()).getId();
        final MoveOp moveOp = new MoveOp(srcId, dstId, tableId, numBlocksToMove);

        dag.addVertex(moveOp);
        moveOpsInCurrStage.add(moveOp);
        opCounter.getAndIncrement();

        // create dependency relationships with every vertices in previous stage.
        moveOpsInPrevStage.forEach(moveOpInPrevStage -> dag.addEdge(moveOpInPrevStage, moveOp));
      }
      moveOpsInPrevStage.clear();
      moveOpsInPrevStage.addAll(moveOpsInCurrStage);
      moveOpsInCurrStage.clear();
    }

    return new ETPlan(dag, opCounter.get());
  }


  /**
   * Build an {@link ETPlan} that makes an associator by given {@code executorId, tableId},
   * migrating blocks from existing associators.
   * This method follows the policy of moving blocks that all blocks are evenly distributed by associators.
   * @param executorId an identifier of executor
   * @param tableId an identifier of table
   * @param executorConf an executor configuration
   * @param associators a list of associators.
   * @return a {@link ETPlan}
   * @throws TableNotExistException when a table with {@code tableId} does not exist
   */
  ETPlan getAddOneAssociatorWithMovesPlan(final String executorId,
                                          final String tableId,
                                          final ExecutorConfiguration executorConf,
                                          final List<AllocatedExecutor> associators) throws TableNotExistException {
    final DAG<Op> dag = new DAGImpl<>();
    final AtomicInteger opCounter = new AtomicInteger(0);

    final AllocateOp allocateOp = new AllocateOp(executorId, executorConf);
    dag.addVertex(allocateOp);
    opCounter.getAndIncrement();

    final AssociateOp associateOp = new AssociateOp(executorId, tableId);
    dag.addVertex(associateOp);
    dag.addEdge(allocateOp, associateOp);
    opCounter.getAndIncrement();

    final AllocatedTable table = etMaster.getTable(tableId);

    // it assumes that blocks were evenly distributed across {@code associators}
    associators.forEach(associator -> {
      final int divisor = associators.size() + 1;
      final int numBlocks = table.getPartitionInfo().get(associator.getId()).size();
      final int numBlocksToMove = numBlocks / divisor;
      final String srcId = associator.getId();
      final String dstId = executorId;

      final MoveOp moveOp = new MoveOp(srcId, dstId, tableId, numBlocksToMove);
      dag.addVertex(moveOp);
      dag.addEdge(associateOp, moveOp);
      opCounter.getAndIncrement();
    });

    return new ETPlan(dag, opCounter.get());
  }

  /**
   * Build an {@link ETPlan} that deletes a randomly selected executor, migrating blocks to remaining executors.
   * This method follows the policy of moving blocks that all blocks are evenly distributed by associators.
   * @param tableId an identifier of table
   * @param associators a list of associators
   * @return a {@link ETPlan}
   * @throws TableNotExistException when a table with {@code tableId} does not exist
   */
  ETPlan getDelOneAssociatorWithMovesPlan(final String tableId,
                                          final List<AllocatedExecutor> associators) throws TableNotExistException {
    final String executorId = pickRandomExecutor(associators).getId();
    final DAG<Op> dag = new DAGImpl<>();
    final AtomicInteger opCounter = new AtomicInteger(0);

    final DeallocateOp deallocateOp = new DeallocateOp(executorId);
    dag.addVertex(deallocateOp);
    opCounter.getAndIncrement();

    final UnassociateOp unassociateOp = new UnassociateOp(executorId, tableId);
    dag.addVertex(unassociateOp);
    dag.addEdge(unassociateOp, deallocateOp);
    opCounter.getAndIncrement();

    final AllocatedTable table = etMaster.getTable(tableId);
    final int divisor = associators.size() - 1;
    final int numBlocks = table.getPartitionInfo().get(executorId).size();
    int remainder = numBlocks % divisor;

    for (final AllocatedExecutor associator : associators) {

      // it must not move blocks to executor which will be deleted.
      if (associator.getId().equals(executorId)) {
        continue;
      }
      final String srcId = executorId;
      final String dstId = associator.getId();

      // remainder are distributed to associator one by one.
      final int numBlocksToMove = remainder == 0 ? numBlocks / divisor : numBlocks / divisor + 1;
      if (remainder > 0) {
        remainder--;
      }

      final MoveOp moveOp = new MoveOp(srcId, dstId, tableId, numBlocksToMove);
      dag.addVertex(moveOp);
      dag.addEdge(moveOp, unassociateOp);
      opCounter.getAndIncrement();
    }

    return new ETPlan(dag, opCounter.get());
  }

  private AllocatedExecutor pickRandomExecutor(final List<AllocatedExecutor> executors) {
    final Random random = new Random();
    final int randomIdx = random.nextInt(executors.size());
    return executors.get(randomIdx);
  }
}
