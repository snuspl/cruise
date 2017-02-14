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
package edu.snu.cay.services.et.driver.impl;

import edu.snu.cay.services.et.configuration.parameters.NumTotalBlocks;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Correctness check for BlockManager.
 */
public final class BlockManagerTest {

  private static final String EXECUTOR_ID_PREFIX = "executor-";
  private static final int NUM_TOTAL_BLOCKS = 1024;
  private static final int NUM_INIT_EXECUTORS = 4;
  private static final int NUM_EXECUTORS_TO_ADD = 5;

  private BlockManager blockManager;

  @Before
  public void setUp() throws InjectionException {
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumTotalBlocks.class, Integer.toString(NUM_TOTAL_BLOCKS))
        .build();

    blockManager = Tang.Factory.getTang().newInjector(configuration).getInstance(BlockManager.class);

    final Set<String> initExecutors = new HashSet<>(NUM_INIT_EXECUTORS);
    for (int executorId = 0; executorId < NUM_INIT_EXECUTORS; executorId++) {
      initExecutors.add(EXECUTOR_ID_PREFIX + executorId);
    }
    blockManager.init(initExecutors);
  }

  /**
   * Register and deregister evaluators, checking whether the state of BlockManager is correctly updated.
   */
  @Test
  public void testRegisterExecutors() {
    // 1. Confirm that all blocks are assigned to the executors
    assertEquals("Fail to correctly initialize TableManager",
        NUM_INIT_EXECUTORS, blockManager.getAssociatedExecutorIds().size());

    // check the number of block are as expected
    int numTotalBlocks = 0;
    for (int idx = 0; idx < NUM_INIT_EXECUTORS; idx++) {
      final int numBlocks = blockManager.getNumBlocks(EXECUTOR_ID_PREFIX + idx);
      assertTrue(numBlocks > 0);
      numTotalBlocks += numBlocks;
    }
    assertEquals("The number of existing blocks should be same with the one of total blocks generated at init",
        numTotalBlocks, NUM_TOTAL_BLOCKS);

    // try to register same executors ids again
    for (int idx = 0; idx < NUM_INIT_EXECUTORS; idx++) {
      try {
        // do not accept eval ids already registered
        blockManager.registerExecutor(EXECUTOR_ID_PREFIX + idx);
        fail();
      } catch (final RuntimeException e) {
        // SUCCESS
      }
    }

    // try to deregister evals
    for (int idx = 0; idx < NUM_INIT_EXECUTORS; idx++) {
      try {
        // evaluators holding blocks cannot be deregistered
        blockManager.deregisterExecutor(EXECUTOR_ID_PREFIX + idx);
        fail();
      } catch (final RuntimeException e) {
        // SUCCESS;
      }
    }

    // 2. Register an additional evaluator and confirm that the added evaluator has no assigned block
    // and the total number of block never changes
    for (int idx = NUM_INIT_EXECUTORS; idx < NUM_INIT_EXECUTORS + NUM_EXECUTORS_TO_ADD; idx++) {
      blockManager.registerExecutor(EXECUTOR_ID_PREFIX + idx);
    }
    assertEquals("Wrong number of registered evaluators",
        NUM_INIT_EXECUTORS + NUM_EXECUTORS_TO_ADD, blockManager.getAssociatedExecutorIds().size());

    // check the number of blocks are as expected
    numTotalBlocks = 0;
    for (int idx = 0; idx < NUM_INIT_EXECUTORS + NUM_EXECUTORS_TO_ADD; idx++) {
      final int numBlocks = blockManager.getNumBlocks(EXECUTOR_ID_PREFIX + idx);
      assertTrue(idx >= NUM_INIT_EXECUTORS ? numBlocks == 0 : numBlocks > 0);
      numTotalBlocks += numBlocks;
    }
    assertEquals("The number of existing blocks should be always same with the one of total blocks generated at init",
        numTotalBlocks, NUM_TOTAL_BLOCKS);

    // deregister all added evals
    for (int idx = NUM_INIT_EXECUTORS; idx < NUM_INIT_EXECUTORS + NUM_EXECUTORS_TO_ADD; idx++) {
      blockManager.deregisterExecutor(EXECUTOR_ID_PREFIX + idx);
    }
    assertEquals("Wrong number of registered evaluators",
        NUM_INIT_EXECUTORS, blockManager.getAssociatedExecutorIds().size());
  }

  /**
   * Move blocks between evaluators including all initial evaluators and a single additional evaluator.
   * The test assumes there are at least 2 initial evaluators.
   */
  @Test
  public void testMoveBlocks() {
    // 1. Move half of block from src evaluator to dest evaluator and
    // confirm that the number of blocks in both evaluators are as expected
    final int srcExecutorIdx = 0;
    final int destExecutorIdx = NUM_INIT_EXECUTORS - 1;

    final String srcExecutorId = EXECUTOR_ID_PREFIX + srcExecutorIdx;
    final String dstExecutorId = EXECUTOR_ID_PREFIX + destExecutorIdx;
    final int numBlockInSrcEval = blockManager.getNumBlocks(srcExecutorId);
    final int numBlockInDestEval = blockManager.getNumBlocks(dstExecutorId);

    final int numBlocksToMove = numBlockInSrcEval / 2;

    List<Integer> blocksToMove = blockManager.chooseBlocksToMove(srcExecutorId, numBlocksToMove);
    assertEquals("Blocks have to be chosen as many as the requested number, when there are enough blocks",
        numBlocksToMove, blocksToMove.size());

    for (final int blockId : blocksToMove) {
      blockManager.updateOwner(blockId, srcExecutorId, dstExecutorId);
      blockManager.releaseBlockFromMove(blockId);
    }
    assertEquals("The number of blocks in the source evaluator decreases as the number of moved blocks",
        numBlockInSrcEval - blocksToMove.size(), blockManager.getNumBlocks(srcExecutorId));
    assertEquals("The number of blocks in the destination evaluator increases as the number of moved blocks",
        numBlockInDestEval + blocksToMove.size(), blockManager.getNumBlocks(dstExecutorId));

    // 2. Move all blocks to the new evaluator and deregister other evaluators,
    // and try to deregister the new evaluator holding all blocks, which should fail
    final int newExecutorIdx = NUM_INIT_EXECUTORS;
    final String newExecutorId = EXECUTOR_ID_PREFIX + newExecutorIdx;
    blockManager.registerExecutor(newExecutorId);

    int numTotalMovedBlocks = 0;

    for (int idx = 0; idx < NUM_INIT_EXECUTORS; idx++) {
      final String executorId = EXECUTOR_ID_PREFIX + idx;
      final int numBlocks = blockManager.getNumBlocks(executorId);
      blocksToMove = blockManager.chooseBlocksToMove(executorId, numBlocks);
      assertEquals("Blocks have to be chosen as many as the requested number, when there are enough blocks",
          numBlocks, blocksToMove.size());

      for (final int blockId : blocksToMove) {
        blockManager.updateOwner(blockId, executorId, newExecutorId);
        blockManager.releaseBlockFromMove(blockId);
      }

      numTotalMovedBlocks += blocksToMove.size();
      assertEquals("The source evaluator should be empty", 0, blockManager.getNumBlocks(executorId));
      assertEquals("The number of blocks in the destination evaluator increases as the number of moved blocks",
          numTotalMovedBlocks, blockManager.getNumBlocks(newExecutorId));

      blockManager.deregisterExecutor(executorId);
    }

    // try to deregister the evaluator holding all blocks
    try {
      blockManager.deregisterExecutor(newExecutorId);
      fail();
    } catch (final RuntimeException e) {
      // SUCCESS
    }
  }
}
