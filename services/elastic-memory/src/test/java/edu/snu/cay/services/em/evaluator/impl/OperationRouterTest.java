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

import edu.snu.cay.services.em.avro.AvroElasticMemoryMessage;
import edu.snu.cay.services.em.avro.RoutingTableInitReqMsg;
import edu.snu.cay.services.em.avro.Type;
import edu.snu.cay.services.em.common.parameters.AddedEval;
import edu.snu.cay.services.em.common.parameters.MemoryStoreId;
import edu.snu.cay.services.em.common.parameters.NumInitialEvals;
import edu.snu.cay.services.em.common.parameters.NumTotalBlocks;
import edu.snu.cay.services.em.driver.impl.BlockManager;
import edu.snu.cay.services.em.driver.impl.ElasticMemoryMsgHandler;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.utils.ThreadUtils;
import org.apache.reef.io.network.impl.NSMessage;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.htrace.TraceInfo;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * Tests to check whether OperationRouter is initialized correctly, and routes operations to the correct target.
 */
public class OperationRouterTest {
  // TODO #509: EM assumes that the eval prefix has "-" at the end
  private static final String EVAL_ID_PREFIX = "EVAL-";

  private ElasticMemoryMsgSender evalMsgSender;
  private CountDownLatch initLatch;

  /**
   * Creates an instance of OperationRouter based on the input parameters.
   * @param numInitialEvals the number of initial evaluators
   * @param numTotalBlocks the number of total blocks
   * @param memoryStoreId the local memory store id
   * @param addedEval a boolean representing whether or not an evaluator is added by EM.add()
   * @return an instance of OperationRouter
   * @throws InjectionException
   */
  private OperationRouter newOperationRouter(final int numInitialEvals,
                                             final int numTotalBlocks,
                                             final int memoryStoreId,
                                             final boolean addedEval) throws InjectionException {
    // 1. setup eval-side components that is common for static and dynamic routers
    final Configuration evalConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumInitialEvals.class, Integer.toString(numInitialEvals))
        .bindNamedParameter(NumTotalBlocks.class, Integer.toString(numTotalBlocks))
        .bindNamedParameter(MemoryStoreId.class, Integer.toString(memoryStoreId))
        .bindNamedParameter(AddedEval.class, Boolean.toString(addedEval))
        .build();
    final Injector evalInjector = Tang.Factory.getTang().newInjector(evalConf);

    evalMsgSender = mock(ElasticMemoryMsgSender.class);
    evalInjector.bindVolatileInstance(ElasticMemoryMsgSender.class, evalMsgSender);
    final OperationRouter router = evalInjector.getInstance(OperationRouter.class);

    // 2. If it is a dynamic router, setup eval-side msg sender and driver-side msg sender/handler and block manager.
    // By mocking msg sender and handler in both side, we can simulate more realistic system behavior
    if (addedEval) {
      final Configuration driverConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(NumTotalBlocks.class, Integer.toString(numTotalBlocks))
          .bindNamedParameter(AddedEval.class, Boolean.toString(addedEval))
          .bindNamedParameter(MemoryStoreId.class, Integer.toString(memoryStoreId))
          .build();

      final Injector driverInjector = Tang.Factory.getTang().newInjector(driverConf);
      final BlockManager blockManager = driverInjector.getInstance(BlockManager.class);

      // Register all eval to block manager, now dynamic router can obtain the complete routing table
      for (int evalIdx = 0; evalIdx < numInitialEvals; evalIdx++) {
        final String endpointId = EVAL_ID_PREFIX + evalIdx;

        // we assume that BlockManager.registerEvaluator() assigns store ids in the increasing order,
        // so the index of evaluator endpoint is equal to the store id
        blockManager.registerEvaluator(endpointId, numInitialEvals);
      }

      final ElasticMemoryMsgSender driverMsgSender = mock(ElasticMemoryMsgSender.class);
      driverInjector.bindVolatileInstance(ElasticMemoryMsgSender.class, driverMsgSender);

      final ElasticMemoryMsgHandler driverMsgHandler = driverInjector.getInstance(ElasticMemoryMsgHandler.class);

      final IdentifierFactory identifierFactory = driverInjector.getInstance(StringIdentifierFactory.class);

      // dummy ids to generate a network message
      final String driverId = "driver";
      final String evalId = "eval";
      final Identifier evalIdentifier = identifierFactory.getNewInstance(evalId);
      final Identifier driverIdentifier = identifierFactory.getNewInstance(driverId);

      doAnswer(new Answer() {
        @Override
        public Object answer(final InvocationOnMock invocation) throws Throwable {
          Thread.sleep(1000); // delay for fetching the routing table from driver

          final RoutingTableInitReqMsg routingTableInitReqMsg = RoutingTableInitReqMsg.newBuilder()
              .build();

          final AvroElasticMemoryMessage msg = AvroElasticMemoryMessage.newBuilder()
              .setType(Type.RoutingTableInitReqMsg)
              .setSrcId(evalId)
              .setDestId(driverId)
              .setRoutingTableInitReqMsg(routingTableInitReqMsg)
              .build();

          driverMsgHandler.onNext(new NSMessage<>(evalIdentifier, driverIdentifier, msg));
          return null;
        }
      }).when(evalMsgSender).sendRoutingTableInitReqMsg(any(TraceInfo.class));

      // reset initLatch
      initLatch = new CountDownLatch(1);

      // driverMsgHander.onNext will invoke driverMsgSender.sendRoutingTableInitMsg with the routine table
      doAnswer(new Answer() {
        @Override
        public Object answer(final InvocationOnMock invocation) throws Throwable {
          final List<Integer> blockLocations = invocation.getArgumentAt(1, List.class);
          router.initialize(blockLocations);
          initLatch.countDown();
          return null;
        }
      }).when(driverMsgSender).sendRoutingTableInitMsg(anyString(), anyList(), any(TraceInfo.class));
    }

    return router;
  }

  /**
   * Checks whether blocks assigned to each MemoryStore have its unique owner (MemoryStore),
   * and the local blocks acquired from getInitialLocalBlockIds() are routed to the local MemoryStore.
   */
  @Test
  public void testRoutingLocalBlocks() throws InjectionException {
    final int numTotalBlocks = 1024;
    final int numMemoryStores = 4;

    final Set<Integer> totalBlocks = new HashSet<>(numTotalBlocks);

    for (int localStoreId = 0; localStoreId < numMemoryStores; localStoreId++) {
      final OperationRouter<?> operationRouter =
          newOperationRouter(numMemoryStores, numTotalBlocks, localStoreId, false);

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
  public void testMultipleRouters() throws InjectionException {
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
          // TODO #509: remove the assumption on the format of context id
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
  public void testUpdatingOwnership() throws InjectionException {
    final int numTotalBlocks = 1024;
    final int numInitialMemoryStores = 4;

    final int srcStoreId = 0;
    final OperationRouter<?> srcRouter = newOperationRouter(numInitialMemoryStores, numTotalBlocks, srcStoreId, false);

    final List<Integer> srcInitialBlocks = srcRouter.getInitialLocalBlockIds();
    List<Integer> srcCurrentBlocks = srcRouter.getCurrentLocalBlockIds();

    assertEquals("Router is initialized incorrectly", srcInitialBlocks.size(), srcCurrentBlocks.size());
    assertTrue("Router is initialized incorrectly", srcInitialBlocks.containsAll(srcCurrentBlocks));

    final int destStoreId = 1;
    final OperationRouter<?> destRouter =
        newOperationRouter(numInitialMemoryStores, numTotalBlocks, destStoreId, false);

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

  /**
   * Tests router after explicitly initializing the routing table.
   * Stores in added evaluators are initialized by communicating with the driver,
   * and stores in initial evaluators are initialized by itself without any communication.
   * The test runs multiple threads using router to check
   * whether the correct result is given, and whether initialization is performed only once or not.
   */
  @Test
  public void testRouterInAddedEvalAfterExplicitInit() throws InjectionException, InterruptedException {
    final int numTotalBlocks = 1024;
    final int numInitialMemoryStores = 4;
    final int numThreads = 8;

    final CountDownLatch threadLatch = new CountDownLatch(numThreads);

    // because we compare the whole set of routers in other tests,
    // this test focus on comparing only two routers initialized in different ways
    final int initStoreId = 0;
    final int addedStoreId = 4; // It should be larger than the largest index of initial stores

    // router in initStore will be initialized statically
    final OperationRouter<?> routerInInitStore
        = newOperationRouter(numInitialMemoryStores, numTotalBlocks, initStoreId, false);
    // router in addedStore will be initialized dynamically
    final OperationRouter<?> routerInAddedStore
        = newOperationRouter(numInitialMemoryStores, numTotalBlocks, addedStoreId, true);

    // we assume that store ids are assigned in the increasing order,
    // so the index of evaluator endpoint is equal to the store id
    final String endpointIdForInitEval = EVAL_ID_PREFIX + initStoreId;
    final String endpointIdForAddedEval = EVAL_ID_PREFIX + addedStoreId;

    routerInInitStore.initialize(endpointIdForInitEval);
    routerInAddedStore.initialize(endpointIdForAddedEval); // It requests the routing table to driver

    // confirm that the router is initialized
    assertTrue(initLatch.await(10, TimeUnit.SECONDS));
    verify(evalMsgSender, times(1)).sendRoutingTableInitReqMsg(any(TraceInfo.class));

    // While multiple threads use router, the initialization never be triggered because it's already initialized.
    final Runnable[] threads = new Runnable[numThreads];

    // though init router is statically initialized and added router is dynamically initialized,
    // because there were no changes in the routing table their views should be equal
    for (int idx = 0; idx < numThreads; idx++) {
      threads[idx] = new Runnable() {
        @Override
        public void run() {
          for (int blockId = 0; blockId < numTotalBlocks; blockId++) {
            final Optional<String> evalIdFromInitRouter = routerInInitStore.resolveEval(blockId);
            final Optional<String> evalIdFromAddedRouter = routerInAddedStore.resolveEval(blockId);

            if (!evalIdFromInitRouter.isPresent()) { // routerInInitStore is local
              assertEquals(endpointIdForInitEval, evalIdFromAddedRouter.get());
            } else if (!evalIdFromAddedRouter.isPresent()) { // routerInAddedStore is local
              assertEquals(endpointIdForAddedEval, evalIdFromInitRouter.get());
            } else {
              assertEquals(evalIdFromInitRouter.get(), evalIdFromAddedRouter.get());
            }
          }
          threadLatch.countDown();
        }
      };
    }

    ThreadUtils.runConcurrently(threads);
    assertTrue(threadLatch.await(30, TimeUnit.SECONDS));

    verify(evalMsgSender, times(1)).sendRoutingTableInitReqMsg(any(TraceInfo.class));
  }

  /**
   * Tests router without explicit initialization of the routing table.
   * Stores in added evaluators are initialized by communicating with the driver,
   * and stores in initial evaluators are initialized by itself without any communication.
   * When a router in an added evaluator is not initialized at the beginning,
   * its initialization will be triggered by {@link OperationRouter#resolveEval(int)}.
   * The test runs multiple threads using router to check
   * whether the correct result is given, and whether initialization is performed only once or not.
   */
  @Test
  public void testRouterInAddedEvalWithoutExplicitInit() throws InjectionException, InterruptedException {
    final int numTotalBlocks = 1024;
    final int numInitialMemoryStores = 4;
    final int numThreads = 8;

    final CountDownLatch threadLatch = new CountDownLatch(numThreads);

    // because we compare the whole set of routers in other tests,
    // this test focus on comparing only two routers initialized in different ways
    final int initStoreId = 0;
    final int addedStoreId = 4; // It should be larger than the largest index of initial stores

    // router in initStore will be initialized statically
    final OperationRouter<?> routerInInitStore
        = newOperationRouter(numInitialMemoryStores, numTotalBlocks, initStoreId, false);
    // router in addedStore will be initialized dynamically
    final OperationRouter<?> routerInAddedStore
        = newOperationRouter(numInitialMemoryStores, numTotalBlocks, addedStoreId, true);

    // Initialization would be done while resolving eval.
    // While multiple threads use router, the initialization should be done only once.
    final Runnable[] threads = new Runnable[numThreads];

    // We need an eval prefix that starts with 'null', because we don't explicitly initialize routers.
    // So the router works with this null prefix.
    // TODO #509: EM assumes that the eval prefix has "-" at the end
    final String evalIdPrefix = null + "-";

    // we assume that store ids are assigned in the increasing order,
    // so the index of evaluator endpoint is equal to the store id
    final String endpointIdForInitEval = evalIdPrefix + initStoreId;
    final String endpointIdForAddedEval = evalIdPrefix + addedStoreId;

    // though init router is statically initialized and added router is dynamically initialized,
    // because there were no changes in the routing table their views should be equal
    for (int idx = 0; idx < numThreads; idx++) {
      threads[idx] = new Runnable() {
        @Override
        public void run() {
          for (int blockId = 0; blockId < numTotalBlocks; blockId++) {
            final Optional<String> evalIdFromInitRouter = routerInInitStore.resolveEval(blockId);
            final Optional<String> evalIdFromAddedRouter = routerInAddedStore.resolveEval(blockId);

            if (!evalIdFromInitRouter.isPresent()) { // routerInInitStore is local
              assertEquals(endpointIdForInitEval, evalIdFromAddedRouter.get());
            } else if (!evalIdFromAddedRouter.isPresent()) { // routerInAddedStore is local
              assertEquals(endpointIdForAddedEval, evalIdFromInitRouter.get());
            } else {
              assertEquals(evalIdFromInitRouter.get(), evalIdFromAddedRouter.get());
            }
          }
          threadLatch.countDown();
        }
      };
    }

    ThreadUtils.runConcurrently(threads);
    assertTrue(threadLatch.await(30, TimeUnit.SECONDS));

    // When router.resolveEval() is called and the router is not initialized yet,
    // it internally invokes router.retryInitialization()
    // that finally invokes router.requestRoutingTable().
    // Because retryInitialization() is a synchronized method, requesting the routing table should be done only once.
    verify(evalMsgSender, times(1)).sendRoutingTableInitReqMsg(any(TraceInfo.class));
  }
}
