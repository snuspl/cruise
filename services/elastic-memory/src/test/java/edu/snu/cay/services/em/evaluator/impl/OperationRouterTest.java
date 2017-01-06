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

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.common.parameters.AddedEval;
import edu.snu.cay.services.em.common.parameters.MemoryStoreId;
import edu.snu.cay.services.em.common.parameters.NumInitialEvals;
import edu.snu.cay.services.em.common.parameters.NumTotalBlocks;
import edu.snu.cay.services.em.driver.impl.BlockManager;
import edu.snu.cay.services.em.driver.impl.EMMsgHandler;
import edu.snu.cay.services.em.msg.api.EMMsgSender;
import org.apache.reef.io.network.impl.NSMessage;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.htrace.TraceInfo;
//import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * Tests to check whether OperationRouter is initialized correctly, and routes operations to the correct target.
 */
public class OperationRouterTest {
  // TODO #509: EM assumes that the eval prefix has "-" at the end
  private static final String EVAL_ID_PREFIX = "EVAL-";

  private EMMsgSender evalMsgSender;
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
    // 1. setup eval-side components that is common for all routers
    final Configuration evalConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumInitialEvals.class, Integer.toString(numInitialEvals))
        .bindNamedParameter(NumTotalBlocks.class, Integer.toString(numTotalBlocks))
        .bindNamedParameter(MemoryStoreId.class, Integer.toString(memoryStoreId))
        .bindNamedParameter(AddedEval.class, Boolean.toString(addedEval))
        .build();
    final Injector evalInjector = Tang.Factory.getTang().newInjector(evalConf);

    evalMsgSender = mock(EMMsgSender.class);
    evalInjector.bindVolatileInstance(EMMsgSender.class, evalMsgSender);
    final OperationRouter router = evalInjector.getInstance(OperationRouter.class);

    // 2. If it is a router for a newly added evaluator,
    // setup eval-side msg sender and driver-side msg sender/handler and block manager.
    // By mocking msg sender and handler in both side, we can simulate more realistic system behaviors.
    if (addedEval) {
      final Configuration driverConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(NumTotalBlocks.class, Integer.toString(numTotalBlocks))
          .bindNamedParameter(AddedEval.class, Boolean.toString(addedEval))
          .bindNamedParameter(MemoryStoreId.class, Integer.toString(memoryStoreId))
          .build();

      final Injector driverInjector = Tang.Factory.getTang().newInjector(driverConf);
      final BlockManager blockManager = driverInjector.getInstance(BlockManager.class);

      // Register all eval to block manager, now this router can obtain the complete routing table
      for (int evalIdx = 0; evalIdx < numInitialEvals; evalIdx++) {
        final String endpointId = EVAL_ID_PREFIX + evalIdx;

        // we assume that BlockManager.registerEvaluator() assigns store ids in the increasing order,
        // so the index of evaluator endpoint is equal to the store id
        blockManager.registerEvaluator(endpointId, numInitialEvals);
      }

      final EMMsgSender driverMsgSender = mock(EMMsgSender.class);
      driverInjector.bindVolatileInstance(EMMsgSender.class, driverMsgSender);

      final EMMsgHandler driverMsgHandler = driverInjector.getInstance(EMMsgHandler.class);

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
              .setEvalId(evalId)
              .build();

          final RoutingTableMsg routingTableMsg = RoutingTableMsg.newBuilder()
              .setType(RoutingTableMsgType.RoutingTableInitReqMsg)
              .setRoutingTableInitReqMsg(routingTableInitReqMsg)
              .build();

          final EMMsg msg = EMMsg.newBuilder()
              .setType(EMMsgType.RoutingTableMsg)
              .setRoutingTableMsg(routingTableMsg)
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
//          router.initRoutingTableWithDriver(blockLocations);
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
//  @Test
  public void testRoutingLocalBlocks() throws InjectionException {
    final int numTotalBlocks = 1024;
    final int numMemoryStores = 4;

    final Set<Integer> totalBlocks = new HashSet<>(numTotalBlocks);

    for (int localStoreId = 0; localStoreId < numMemoryStores; localStoreId++) {
      final OperationRouter<?> operationRouter =
          newOperationRouter(numMemoryStores, numTotalBlocks, localStoreId, false);

//      final List<Integer> localBlockIds = operationRouter.getInitialLocalBlockIds();
//
//      for (final int blockId : localBlockIds) {
//        // OperationRouter.resolveEval(blockId) returns empty when the MemoryStore owns the block locally
//        assertFalse("Router fails to classify local blocks",
//            operationRouter.resolveEval(blockId).isPresent());
//        assertTrue("The same block is owned by multiple stores", totalBlocks.add(blockId));
//      }
    }

    assertEquals("There are missing blocks", numTotalBlocks, totalBlocks.size());
  }
}
