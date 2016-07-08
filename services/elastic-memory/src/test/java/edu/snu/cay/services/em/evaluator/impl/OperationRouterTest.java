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
package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.common.parameters.AddedEval;
import edu.snu.cay.services.em.common.parameters.MemoryStoreId;
import edu.snu.cay.services.em.common.parameters.NumInitialEvals;
import edu.snu.cay.services.em.common.parameters.NumTotalBlocks;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.Optional;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * Tests to check whether OperationRouter is initialized correctly, and routes operations to the correct target.
 */
public class OperationRouterTest {
  private OperationRouter newOperationRouter(final int numInitialEvals,
                                             final int numTotalBlocks,
                                             final int memoryStoreId,
                                             final boolean addedEval) {
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumInitialEvals.class, Integer.toString(numInitialEvals))
        .bindNamedParameter(NumTotalBlocks.class, Integer.toString(numTotalBlocks))
        .bindNamedParameter(MemoryStoreId.class, Integer.toString(memoryStoreId))
        .bindNamedParameter(AddedEval.class, Boolean.toString(addedEval))
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);

    final ElasticMemoryMsgSender msgSender = mock(ElasticMemoryMsgSender.class);
    injector.bindVolatileInstance(ElasticMemoryMsgSender.class, msgSender);

    try {
      return injector.getInstance(OperationRouter.class);
    } catch (InjectionException e) {
      throw new RuntimeException("InjectionException while getting router instance");
    }
  }

  /**
   * Checks whether blocks assigned to each MemoryStore have its unique owner (MemoryStore),
   * and the local blocks acquired from getInitialLocalBlockIds() are routed to the local MemoryStore.
   */
  @Test
  public void testRoutingLocalBlocks() {
    final int numTotalBlocks = 1024;
    final int numMemoryStores = 4;

    final Set<Integer> totalBlocks = new HashSet<>(numTotalBlocks);

    for (int localStoreId = 0; localStoreId < numMemoryStores; localStoreId++) {
      final OperationRouter<?> operationRouter = newOperationRouter(numMemoryStores, numTotalBlocks, localStoreId, false);

      final List<Integer> localBlockIds = operationRouter.getInitialLocalBlockIds();

      for (final int blockId : localBlockIds) {
        // OperationRouter.resolveEval(blockId) returns empty when the MemoryStore owns the block locally
        assertEquals("Router fails to classify local blocks", Optional.empty(), operationRouter.resolveEval(blockId));
        assertTrue("The same block is owned by multiple stores", totalBlocks.add(blockId));
      }
    }

    assertEquals("There are missing blocks", numTotalBlocks, totalBlocks.size());
  }

  /**
   * Checks whether MemoryStores share the same routing table initially.
   */
  @Test
  public void testMultipleRouters() {
    final int numTotalBlocks = 1024;
    final int numMemoryStores = 4;

    final OperationRouter<?>[] routers = new OperationRouter[numMemoryStores];
    for (int storeId = 0; storeId < numMemoryStores; storeId++) {
      routers[storeId] = newOperationRouter(numMemoryStores, numTotalBlocks, storeId, false);
    }

    for (int blockId = 0; blockId < numTotalBlocks; blockId++) {

      // This is the memory store id that is answered at the first time.
      // It is for checking all routers give the same answer.
      // -1 means that memory store id for the block has not been found yet
      int firstAnswer = -1;

      boolean localStoreFound = false;

      // check all routers give same answer
      for (int storeId = 0; storeId < numMemoryStores; storeId++) {
        final Optional<String> evalId = routers[storeId].resolveEval(blockId);

        final int targetStoreId;
        // OperationRouter.resolveEval(blockId) returns empty when the MemoryStore owns the block locally
        if (!evalId.isPresent()) {
          assertFalse("Block should belong to only one store", localStoreFound);
          localStoreFound = true;

          targetStoreId = storeId;
        } else {
          targetStoreId = Integer.valueOf(evalId.get().split("-")[1]);
        }

        if (firstAnswer == -1) {
          firstAnswer = targetStoreId; // it's set by the first router's answer
        } else {
          assertEquals("Routers should give the same memory store id for the same block", firstAnswer, targetStoreId);
        }
      }
    }
  }

  /**
   * Tests whether routers are correctly updated by {@link OperationRouter#updateOwnership(int, int, int)}.
   */
  @Test
  public void testUpdatingOwnership() {
    final int numTotalBlocks = 1024;
    final int numInitialMemoryStores = 4;

    final int srcStoreId = 0;
    final OperationRouter<?> srcRouter = newOperationRouter(numInitialMemoryStores, numTotalBlocks, srcStoreId, false);

    final List<Integer> srcInitialBlocks = srcRouter.getInitialLocalBlockIds();
    List<Integer> srcCurrentBlocks = srcRouter.getCurrentLocalBlockIds();

    assertEquals("Router is initialized incorrectly", srcInitialBlocks.size(), srcCurrentBlocks.size());
    assertTrue("Router is initialized incorrectly", srcInitialBlocks.containsAll(srcCurrentBlocks));

    final int destStoreId = 1;
    final OperationRouter<?> destRouter = newOperationRouter(numInitialMemoryStores, numTotalBlocks, destStoreId, false);

    // move the half of blocks between two evaluators by updating routers
    final int numBlocksToMove = srcInitialBlocks.size() / 2;
    final List<Integer> movedBlocks = new ArrayList<>(numBlocksToMove);
    for (int i = 0; i < numBlocksToMove; i++) {
      final int movingBlockId = srcInitialBlocks.get(i);
      srcRouter.updateOwnership(movingBlockId, srcStoreId, destStoreId);
      destRouter.updateOwnership(movingBlockId, srcStoreId, destStoreId);
      movedBlocks.add(movingBlockId);
    }

    // check that the router is correctly updated as expected
    srcCurrentBlocks = srcRouter.getCurrentLocalBlockIds();
    final List<Integer> destCurrentBlocks = destRouter.getCurrentLocalBlockIds();
    final List<Integer> destInitialBlocks = destRouter.getInitialLocalBlockIds();

    assertEquals("The number of current blocks in source router has not been updated correctly",
        srcInitialBlocks.size() - numBlocksToMove, srcCurrentBlocks.size());
    assertEquals("The number of current blocks in destination router has not been updated correctly",
        destInitialBlocks.size() + numBlocksToMove, destCurrentBlocks.size());
    assertTrue("Current blocks in source router have not been updated correctly",
        srcInitialBlocks.containsAll(srcCurrentBlocks));
    assertTrue("Current blocks in destination router have not been updated correctly",
        destCurrentBlocks.containsAll(destInitialBlocks));

    for (final int blockId : movedBlocks) {
      assertFalse("This block should have been moved out from source router", srcCurrentBlocks.contains(blockId));
      assertTrue("This block should have been moved into destination router", destCurrentBlocks.contains(blockId));
    }
  }
}
