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
package edu.snu.cay.dolphin.async.plan;

import edu.snu.cay.dolphin.async.DolphinDriver;
import edu.snu.cay.dolphin.async.DolphinMaster;
import edu.snu.cay.dolphin.async.optimizer.api.DataInfo;
import edu.snu.cay.dolphin.async.optimizer.impl.DataInfoImpl;
import edu.snu.cay.dolphin.async.optimizer.parameters.Constants;
import edu.snu.cay.dolphin.async.plan.api.Plan;
import edu.snu.cay.dolphin.async.plan.impl.PlanCompiler;
import edu.snu.cay.dolphin.async.plan.impl.PlanImpl;
import edu.snu.cay.dolphin.async.plan.impl.TransferStepImpl;
import edu.snu.cay.services.et.plan.api.Op;
import edu.snu.cay.services.et.plan.impl.ETPlan;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.mockito.Mockito.mock;
import static org.junit.Assert.*;

/**
 * A test for {@link PlanCompiler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({DolphinMaster.class, DolphinDriver.class})
public class PlanCompilerTest {
  private static final Logger LOG = Logger.getLogger(PlanCompilerTest.class.getName());
  private static final String EVAL_ID_PREFIX = "EVAL-";
  private static final DataInfo DUMMY_DATA_INFO = new DataInfoImpl();

  private PlanCompiler compiler;

  @Before
  public void setup() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();

    injector.bindVolatileInstance(DolphinMaster.class, mock(DolphinMaster.class));
    injector.bindVolatileInstance(DolphinDriver.class, mock(DolphinDriver.class));

    compiler = injector.getInstance(PlanCompiler.class);
  }

  @Test
  public void testMovePlan() {
    final Plan plan = PlanImpl.newBuilder()
        .addTransferStep(Constants.NAMESPACE_WORKER,
            new TransferStepImpl(EVAL_ID_PREFIX + 0, EVAL_ID_PREFIX + 1, DUMMY_DATA_INFO))
        .addTransferStep(Constants.NAMESPACE_WORKER,
            new TransferStepImpl(EVAL_ID_PREFIX + 2, EVAL_ID_PREFIX + 3, DUMMY_DATA_INFO))
        .addTransferStep(Constants.NAMESPACE_SERVER,
            new TransferStepImpl(EVAL_ID_PREFIX + 4, EVAL_ID_PREFIX + 5, DUMMY_DATA_INFO))
        .addTransferStep(Constants.NAMESPACE_SERVER,
            new TransferStepImpl(EVAL_ID_PREFIX + 6, EVAL_ID_PREFIX + 7, DUMMY_DATA_INFO))
        .build();

    final int numAvailableEvals = 0; // no meaning because this plan has no change about resource (executor)
    final ETPlan etPlan = compiler.compile(plan, numAvailableEvals);

    // moves have no dependency
    final Set<Op> initialOps = etPlan.getInitialOps();
    assertEquals(4, initialOps.size());
    initialOps.forEach(op -> {
      assertEquals(Op.OpType.MOVE, op.getOpType());
      assertTrue(etPlan.onComplete(op).isEmpty());
    });
  }

  @Test
  public void testAddOneWorkerPlan() {
    final String workerId = EVAL_ID_PREFIX + 0;
    final Plan plan = PlanImpl.newBuilder()
        .addEvaluatorsToAdd(Constants.NAMESPACE_WORKER, Collections.singletonList(workerId))
        .addTransferStep(Constants.NAMESPACE_WORKER,
            new TransferStepImpl(EVAL_ID_PREFIX + 1, workerId, DUMMY_DATA_INFO))
        .build();

    final int numAvailableEvals = 1; // should larger than or equal to 1 for adding a new worker
    final ETPlan etPlan = compiler.compile(plan, numAvailableEvals);

    // Adding one worker involves: allocate, associate, move, subscribe, start
    assertEquals(5, etPlan.getNumTotalOps());

    // check dependency
    // first stage: allocate
    final Set<Op> initialOps = etPlan.getInitialOps();
    assertEquals(1, initialOps.size());
    final Op initialOp = initialOps.iterator().next();
    assertEquals(Op.OpType.ALLOCATE, initialOp.getOpType());

    // second stage: subscribe and associate
    final Set<Op> secondOps = etPlan.onComplete(initialOp);
    assertEquals(2, secondOps.size());
    final Iterator<Op> iter = secondOps.iterator();
    final Op secondOp0 = iter.next();
    final Op secondOp1 = iter.next();

    // one operation should be SUBSCRIBE and the other should be ASSOCIATE
    assertTrue(secondOp0.getOpType().equals(Op.OpType.SUBSCRIBE) || secondOp0.getOpType().equals(Op.OpType.ASSOCIATE));
    assertTrue(secondOp1.getOpType().equals(Op.OpType.SUBSCRIBE) || secondOp1.getOpType().equals(Op.OpType.ASSOCIATE));
    assertNotEquals(secondOp0.getOpType(), secondOp1.getOpType());

    final Op subscribeOp = secondOp0.getOpType().equals(Op.OpType.SUBSCRIBE) ? secondOp0 : secondOp1;
    final Op associateOp = secondOp0.getOpType().equals(Op.OpType.ASSOCIATE) ? secondOp0 : secondOp1;

    // third stage: move
    final Set<Op> thirdOps = etPlan.onComplete(associateOp);
    assertEquals(1, thirdOps.size());
    final Op thirdOp = thirdOps.iterator().next();
    assertEquals(Op.OpType.MOVE, thirdOp.getOpType());

    assertTrue(etPlan.onComplete(thirdOp).isEmpty());

    // last stage: start
    final Set<Op> finalOps = etPlan.onComplete(subscribeOp);
    assertEquals(1, finalOps.size());
    final Op finalOp = finalOps.iterator().next();
    assertEquals(Op.OpType.START, finalOp.getOpType());

    assertTrue(etPlan.onComplete(finalOp).isEmpty());
  }

  @Test
  public void testAddOneServerPlan() {
    final String serverId = EVAL_ID_PREFIX + 0;
    final Plan plan = PlanImpl.newBuilder()
        .addEvaluatorsToAdd(Constants.NAMESPACE_SERVER, Collections.singletonList(serverId))
        .addTransferStep(Constants.NAMESPACE_SERVER,
            new TransferStepImpl(EVAL_ID_PREFIX + 1, serverId, DUMMY_DATA_INFO))
        .build();

    final int numAvailableEvals = 1; // should larger than or equal to 1 for adding a new server
    final ETPlan etPlan = compiler.compile(plan, numAvailableEvals);

    // Adding one server involves: allocate, associate, move, start
    assertEquals(4, etPlan.getNumTotalOps());

    // check dependency
    // first stage: allocate
    final Set<Op> initialOps = etPlan.getInitialOps();
    assertEquals(1, initialOps.size());
    final Op initialOp = initialOps.iterator().next();
    assertEquals(Op.OpType.ALLOCATE, initialOp.getOpType());

    // second stage: associate
    final Set<Op> secondOps = etPlan.onComplete(initialOp);
    assertEquals(1, secondOps.size());
    final Op secondOp = secondOps.iterator().next();
    assertEquals(Op.OpType.ASSOCIATE, secondOp.getOpType());

    // third stage: move
    final Set<Op> thirdOps = etPlan.onComplete(secondOp);
    assertEquals(1, thirdOps.size());
    final Op thirdOp = thirdOps.iterator().next();
    assertEquals(Op.OpType.MOVE, thirdOp.getOpType());

    // last stage: start
    final Set<Op> finalOps = etPlan.onComplete(thirdOp);
    assertEquals(1, finalOps.size());
    final Op finalOp = finalOps.iterator().next();
    assertEquals(Op.OpType.START, finalOp.getOpType());

    assertTrue(etPlan.onComplete(finalOp).isEmpty());
  }

  @Test
  public void testDelOneWorkerPlan() {
    final String workerId = EVAL_ID_PREFIX + 0;
    final Plan plan = PlanImpl.newBuilder()
        .addEvaluatorsToDelete(Constants.NAMESPACE_WORKER, Collections.singletonList(workerId))
        .addTransferStep(Constants.NAMESPACE_WORKER,
            new TransferStepImpl(workerId, EVAL_ID_PREFIX + 1, DUMMY_DATA_INFO))
        .build();

    final int numAvailableEvals = 0; // no meaning because this plan requires no more resources
    final ETPlan etPlan = compiler.compile(plan, numAvailableEvals);

    // Deleting one worker involves: stop, move, unassociate, unsubscribe, deallocate
    assertEquals(5, etPlan.getNumTotalOps());

    // check dependency
    // first stage: stop
    final Set<Op> initialOps = etPlan.getInitialOps();
    assertEquals(1, initialOps.size());
    final Op initialOp = initialOps.iterator().next();
    assertEquals(Op.OpType.STOP, initialOp.getOpType());

    // second stage: unsubscribe and move
    final Set<Op> secondOps = etPlan.onComplete(initialOp);
    assertEquals(2, secondOps.size());
    final Iterator<Op> iter = secondOps.iterator();
    final Op secondOp0 = iter.next();
    final Op secondOp1 = iter.next();

    // one operation should be UNSUBSCRIBE and the other should be MOVE
    assertTrue(secondOp0.getOpType().equals(Op.OpType.UNSUBSCRIBE) ||
        secondOp0.getOpType().equals(Op.OpType.MOVE));
    assertTrue(secondOp1.getOpType().equals(Op.OpType.UNSUBSCRIBE) ||
        secondOp1.getOpType().equals(Op.OpType.MOVE));
    assertNotEquals(secondOp0.getOpType(), secondOp1.getOpType());

    final Op unsubscribeOp = secondOp0.getOpType().equals(Op.OpType.UNSUBSCRIBE) ? secondOp0 : secondOp1;
    final Op moveOp = secondOp0.getOpType().equals(Op.OpType.MOVE) ? secondOp0 : secondOp1;

    assertTrue(etPlan.onComplete(unsubscribeOp).isEmpty());

    // third stage: unassociate
    final Set<Op> thirdOps = etPlan.onComplete(moveOp);
    assertEquals(1, thirdOps.size());
    final Op thirdOp = thirdOps.iterator().next();
    assertEquals(Op.OpType.UNASSOCIATE, thirdOp.getOpType());

    // last stage: deallocate
    final Set<Op> finalOps = etPlan.onComplete(thirdOp);
    assertEquals(1, finalOps.size());
    final Op finalOp = finalOps.iterator().next();
    assertEquals(Op.OpType.DEALLOCATE, finalOp.getOpType());

    assertTrue(etPlan.onComplete(finalOp).isEmpty());
  }

  @Test
  public void testDelOneServerPlan() {
    final String serverId = EVAL_ID_PREFIX + 0;
    final Plan plan = PlanImpl.newBuilder()
        .addEvaluatorsToDelete(Constants.NAMESPACE_SERVER, Collections.singletonList(serverId))
        .addTransferStep(Constants.NAMESPACE_SERVER,
            new TransferStepImpl(serverId, EVAL_ID_PREFIX + 1, DUMMY_DATA_INFO))
        .build();

    final int numAvailableEvals = 0; // no meaning because this plan requires no more resources
    final ETPlan etPlan = compiler.compile(plan, numAvailableEvals);

    // Deleting one server involves: move, unassociate, deallocate, stop
    assertEquals(4, etPlan.getNumTotalOps());

    // check dependency
    // first stage: stop
    final Set<Op> initialOps = etPlan.getInitialOps();
    assertEquals(1, initialOps.size());
    final Op initialOp = initialOps.iterator().next();
    assertEquals(Op.OpType.STOP, initialOp.getOpType());

    // second stage: move
    final Set<Op> secondOps = etPlan.onComplete(initialOp);
    assertEquals(1, secondOps.size());
    final Op secondOp = secondOps.iterator().next();
    assertEquals(Op.OpType.MOVE, secondOp.getOpType());

    // third stage: unassociate
    final Set<Op> thirdOps = etPlan.onComplete(secondOp);
    assertEquals(1, thirdOps.size());
    final Op thirdOp = thirdOps.iterator().next();
    assertEquals(Op.OpType.UNASSOCIATE, thirdOp.getOpType());

    // last stage: deallocate
    final Set<Op> finalOps = etPlan.onComplete(thirdOp);
    assertEquals(1, finalOps.size());
    final Op finalOp = finalOps.iterator().next();
    assertEquals(Op.OpType.DEALLOCATE, finalOp.getOpType());

    assertTrue(etPlan.onComplete(finalOp).isEmpty());
  }

  @Test
  public void testSwitchOneServerToWorkerPlan() {
    final String serverId = EVAL_ID_PREFIX + 0;
    final String workerId = EVAL_ID_PREFIX + 1;
    final Plan plan = PlanImpl.newBuilder()
        .addEvaluatorToDelete(Constants.NAMESPACE_SERVER, serverId)
        .addTransferStep(Constants.NAMESPACE_SERVER,
            new TransferStepImpl(serverId, EVAL_ID_PREFIX + 2, DUMMY_DATA_INFO))
        .addEvaluatorToAdd(Constants.NAMESPACE_WORKER, workerId)
        .addTransferStep(Constants.NAMESPACE_WORKER,
            new TransferStepImpl(EVAL_ID_PREFIX + 2, workerId, DUMMY_DATA_INFO))
        .build();

    final int numAvailableEvals = 0; // no meaning because this plan requires no more resources
    final ETPlan etPlan = compiler.compile(plan, numAvailableEvals);

    LOG.log(Level.INFO, "Plan: {0}", etPlan);

    // Switching server to worker involves following ops
    // to stop server: move, unassociate, stop
    // to start worker: associate, move, subscribe, start
    assertEquals(7, etPlan.getNumTotalOps());

    // check dependency
    // first stage: stop, associateOp
    final Set<Op> initialOps = etPlan.getInitialOps();
    assertEquals(2, initialOps.size());
    final Iterator<Op> iter = initialOps.iterator();
    final Op initialOp0 = iter.next();
    final Op initialOp1 = iter.next();

    // one operation should be SUBSCRIBE and the other should be ASSOCIATE
    assertTrue(initialOp0.getOpType().equals(Op.OpType.STOP) || initialOp0.getOpType().equals(Op.OpType.ASSOCIATE));
    assertTrue(initialOp1.getOpType().equals(Op.OpType.STOP) || initialOp1.getOpType().equals(Op.OpType.ASSOCIATE));
    assertNotEquals(initialOp0.getOpType(), initialOp1.getOpType());

    final Op stopOp = initialOp0.getOpType().equals(Op.OpType.SUBSCRIBE) ? initialOp0 : initialOp1;
    final Op associateOp = initialOp0.getOpType().equals(Op.OpType.ASSOCIATE) ? initialOp0 : initialOp1;

    // 1. start with stopOp and its following ops that are for removing server from this executor
    // second stage: move
    final Set<Op> secondOps0 = etPlan.onComplete(stopOp);
    assertEquals(1, secondOps0.size());
    final Op secondOp0 = secondOps0.iterator().next();
    assertEquals(Op.OpType.MOVE, secondOp0.getOpType());

    // third stage: unassociate
    final Set<Op> thirdOps0 = etPlan.onComplete(secondOp0);
    assertEquals(1, thirdOps0.size());
    final Op thirdOp0 = thirdOps0.iterator().next();
    assertEquals(Op.OpType.UNASSOCIATE, thirdOp0.getOpType());

    // fourth stage: subscribe
    final Set<Op> fourthOps0 = etPlan.onComplete(thirdOp0);
    assertEquals(1, fourthOps0.size());
    final Op fourthOp0 = fourthOps0.iterator().next();
    assertEquals(Op.OpType.SUBSCRIBE, fourthOp0.getOpType());

    assertTrue(etPlan.onComplete(fourthOp0).isEmpty());

    // 2. continue with associateOp and its following ops that are for installing worker to this executor
    // second stage: move
    final Set<Op> secondOps1 = etPlan.onComplete(associateOp);
    assertEquals(1, secondOps1.size());
    final Op secondOp1 = secondOps1.iterator().next();
    assertEquals(Op.OpType.MOVE, secondOp1.getOpType());

    // third stage: start
    final Set<Op> thirdOps1 = etPlan.onComplete(secondOp1);
    assertEquals(1, thirdOps1.size());
    final Op thirdOp1 = thirdOps1.iterator().next();
    assertEquals(Op.OpType.START, thirdOp1.getOpType());

    assertTrue(etPlan.onComplete(thirdOp1).isEmpty());
  }
}
