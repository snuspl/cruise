/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.services.em.driver.impl;

import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Correctness check for BlockManager.
 */
public final class BlockManagerTest {

  private static final String EVAL_ID_PREFIX = "eval-";
  private static final int NUM_INIT_EVALS = 4;

  private BlockManager blockManager;

  @Before
  public void setUp() throws InjectionException {
    blockManager = Tang.Factory.getTang().newInjector().getInstance(BlockManager.class);
  }

  /**
   * Register and deregister evaluators, checking whether the state of BlockManager is correctly updated.
   */
  @Test
  public void testRegisterEvals() {
    assertTrue(blockManager.getActiveEvaluators().isEmpty());

    // 1. Register initial evaluators and confirm that all blocks are assigned to the evaluators
    for (int idx = 0; idx < NUM_INIT_EVALS; idx++) {
      blockManager.registerEvaluator(EVAL_ID_PREFIX + idx, NUM_INIT_EVALS);
    }
    assertEquals(NUM_INIT_EVALS, blockManager.getActiveEvaluators().size());

    // check the number of block are as expected
    int blockSum = 0;
    for (int idx = 0; idx < NUM_INIT_EVALS; idx++) {
      final int numBlocks = blockManager.getNumBlocks(EVAL_ID_PREFIX + idx);
      assertTrue(numBlocks > 0);
      blockSum += numBlocks;
    }
    assertEquals(blockManager.getNumTotalBlocks(), blockSum);
    assertEquals(blockManager.getNumTotalBlocks(), blockManager.getBlockLocations().size());

    // try to register same eval ids again
    for (int idx = 0; idx < NUM_INIT_EVALS; idx++) {
      try {
        // do not accept eval ids already registered
        blockManager.registerEvaluator(EVAL_ID_PREFIX + idx, NUM_INIT_EVALS);
        fail();
      } catch (final RuntimeException e) {
        // SUCCESS
      }
    }

    // try to deregister evals
    for (int idx = 0; idx < NUM_INIT_EVALS; idx++) {
      try {
        // evaluators holding blocks cannot be deregistered
        blockManager.deregisterEvaluator(EVAL_ID_PREFIX + idx);
        fail();
      } catch (final RuntimeException e) {
        // SUCCESS;
      }
    }

    // 2. Register an additional evaluator and confirm that the added evaluator has no assigned block
    // and the total number of block never changes
    final int numAddedEvals = 5;

    for (int idx = NUM_INIT_EVALS; idx < NUM_INIT_EVALS + numAddedEvals; idx++) {
      blockManager.registerEvaluator(EVAL_ID_PREFIX + idx, NUM_INIT_EVALS);
    }
    assertEquals(NUM_INIT_EVALS + numAddedEvals, blockManager.getActiveEvaluators().size());

    // check the number of blocks are as expected
    blockSum = 0;
    for (int idx = 0; idx < NUM_INIT_EVALS + numAddedEvals; idx++) {
      final int numBlocks = blockManager.getNumBlocks(EVAL_ID_PREFIX + idx);
      assertTrue(idx >= NUM_INIT_EVALS ? numBlocks == 0 : numBlocks > 0);
      blockSum += numBlocks;
    }
    assertEquals(blockManager.getNumTotalBlocks(), blockSum);
    assertEquals(blockManager.getNumTotalBlocks(), blockManager.getBlockLocations().size());

    // deregister all added evals
    for (int idx = NUM_INIT_EVALS; idx < NUM_INIT_EVALS + numAddedEvals; idx++) {
      blockManager.deregisterEvaluator(EVAL_ID_PREFIX + idx);
    }
    assertEquals(NUM_INIT_EVALS, blockManager.getActiveEvaluators().size());
    assertEquals(blockManager.getNumTotalBlocks(), blockManager.getBlockLocations().size());
  }

  /**
   * Move blocks between evaluators including all initial evaluators and a single additional evaluator.
   * The test assumes there are at least 2 initial evaluators.
   */
  @Test
  public void testMoveBlocks() {
    assertTrue(blockManager.getActiveEvaluators().isEmpty());

    // 1. Register NUM_INIT_EVALS + 1 evaluators
    for (int idx = 0; idx < NUM_INIT_EVALS + 1; idx++) {
      blockManager.registerEvaluator(EVAL_ID_PREFIX + idx, NUM_INIT_EVALS);
    }
    final int numActiveEvals = NUM_INIT_EVALS + 1;
    assertEquals(numActiveEvals, blockManager.getActiveEvaluators().size());

    final int newEvalIndex = NUM_INIT_EVALS;

    int blockSum = 0;
    for (int idx = 0; idx < numActiveEvals; idx++) {
      final int numBlocks = blockManager.getNumBlocks(EVAL_ID_PREFIX + idx);
      assertTrue(idx == newEvalIndex ? numBlocks == 0 : numBlocks > 0);
      blockSum += numBlocks;
    }
    assertEquals(blockManager.getNumTotalBlocks(), blockSum);
    assertEquals(blockManager.getNumTotalBlocks(), blockManager.getBlockLocations().size());

    // 2. Move half of block from src evaluator to dest evaluator and
    // confirm that the number of blocks in both evaluators are as expected
    final int srcIndex = 0;
    final int destIndex = 1;

    final String srcEvalId = EVAL_ID_PREFIX + srcIndex;
    final String destEvalId = EVAL_ID_PREFIX + destIndex;
    final int numBlockInSrcEval = blockManager.getNumBlocks(srcEvalId);
    final int numBlockInDestEval = blockManager.getNumBlocks(destEvalId);

    final int numBlocksToMove = numBlockInSrcEval / 2;

    List<Integer> blocksToMove = blockManager.chooseBlocksToMove(srcEvalId, numBlocksToMove);
    assertTrue(blocksToMove.size() == numBlocksToMove);

    int srcStoreId = blockManager.getMemoryStoreId(srcEvalId);
    int destStoreId = blockManager.getMemoryStoreId(destEvalId);
    for (final int blockId : blocksToMove) {
      blockManager.updateOwner(blockId, srcStoreId, destStoreId);
      blockManager.releaseBlockFromMove(blockId);
    }
    assertEquals(numBlockInSrcEval - blocksToMove.size(), blockManager.getNumBlocks(srcEvalId));
    assertEquals(numBlockInDestEval + blocksToMove.size(), blockManager.getNumBlocks(destEvalId));

    // 3. Move all blocks to the new evaluator and deregister other evaluators,
    // and try to deregister the new evaluator holding all blocks, which should fail
    final String newEvalId = EVAL_ID_PREFIX + newEvalIndex;
    destStoreId = blockManager.getMemoryStoreId(newEvalId);

    for (int idx = 0; idx < NUM_INIT_EVALS; idx++) {
      final String evalId = EVAL_ID_PREFIX + idx;
      final int numBlocks = blockManager.getNumBlocks(evalId);
      blocksToMove = blockManager.chooseBlocksToMove(evalId, numBlocks);
      srcStoreId = blockManager.getMemoryStoreId(evalId);

      for (final int blockId : blocksToMove) {
        blockManager.updateOwner(blockId, srcStoreId, destStoreId);
        blockManager.releaseBlockFromMove(blockId);
      }

      assertEquals(0, blockManager.getNumBlocks(evalId));
      blockManager.deregisterEvaluator(evalId);
    }
    assertEquals(blockManager.getNumBlocks(newEvalId), blockManager.getNumTotalBlocks());

    // try to deregister the evaluator holding all blocks
    try {
      blockManager.deregisterEvaluator(newEvalId);
      fail();
    } catch (final RuntimeException e) {
      // SUCCESS
    }
  }
}
