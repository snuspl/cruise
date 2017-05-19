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

import edu.snu.cay.dolphin.async.ETDolphinDriver;
import edu.snu.cay.dolphin.async.optimizer.api.DataInfo;
import edu.snu.cay.dolphin.async.optimizer.impl.DataInfoImpl;
import edu.snu.cay.dolphin.async.optimizer.parameters.Constants;
import edu.snu.cay.dolphin.async.plan.api.Plan;
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

import static org.mockito.Mockito.mock;
import static org.junit.Assert.*;

/**
 * A test for {@link PlanCompiler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ETDolphinDriver.class})
public class PlanCompilerTest {
  private static final String EVAL_ID_PREFIX = "EVAL-";
  private static final DataInfo DUMMY_DATA_INFO = new DataInfoImpl();

  private PlanCompiler compiler;

  @Before
  public void setup() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();

    final ETDolphinDriver etDolphinDriver = mock(ETDolphinDriver.class);

    injector.bindVolatileInstance(ETDolphinDriver.class, etDolphinDriver);

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

    // Adding one server involves: allocate, associate, move
    assertEquals(3, etPlan.getNumTotalOps());

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

    // last stage: move
    final Set<Op> finalOps = etPlan.onComplete(secondOp);
    assertEquals(1, finalOps.size());
    final Op finalOp = finalOps.iterator().next();
    assertEquals(Op.OpType.MOVE, finalOp.getOpType());

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

    // Deleting one server involves: move, unassociate, deallocate
    assertEquals(3, etPlan.getNumTotalOps());

    // check dependency
    // first stage: move
    final Set<Op> initialOps = etPlan.getInitialOps();
    assertEquals(1, initialOps.size());
    final Op initialOp = initialOps.iterator().next();
    assertEquals(Op.OpType.MOVE, initialOp.getOpType());

    // second stage: unassociate
    final Set<Op> secondOps = etPlan.onComplete(initialOp);
    assertEquals(1, secondOps.size());
    final Op secondOp = secondOps.iterator().next();
    assertEquals(Op.OpType.UNASSOCIATE, secondOp.getOpType());

    // last stage: deallocate
    final Set<Op> finalOps = etPlan.onComplete(secondOp);
    assertEquals(1, finalOps.size());
    final Op finalOp = finalOps.iterator().next();
    assertEquals(Op.OpType.DEALLOCATE, finalOp.getOpType());

    assertTrue(etPlan.onComplete(finalOp).isEmpty());
  }

  @Test
  public void testDelServerAndAddWorkerPlan() {
    final String serverId = EVAL_ID_PREFIX + 0;
    final String workerId = EVAL_ID_PREFIX + 1;
    final Plan plan = PlanImpl.newBuilder()
        .addEvaluatorsToDelete(Constants.NAMESPACE_SERVER, Collections.singletonList(serverId))
        .addTransferStep(Constants.NAMESPACE_SERVER,
            new TransferStepImpl(serverId, EVAL_ID_PREFIX + 2, DUMMY_DATA_INFO))
        .addEvaluatorsToAdd(Constants.NAMESPACE_WORKER, Collections.singletonList(workerId))
        .addTransferStep(Constants.NAMESPACE_WORKER,
            new TransferStepImpl(EVAL_ID_PREFIX + 2, workerId, DUMMY_DATA_INFO))
        .build();

    final int numAvailableEvals = 0; // by setting is as zero, allocate should be done after deallocate
    final ETPlan etPlan = compiler.compile(plan, numAvailableEvals);

    // Deleting one server involves: move, unassociate, deallocate
    // Adding one worker involves: allocate, associate, move, subscribe, start
    assertEquals(8, etPlan.getNumTotalOps());

    // check dependency
    // first stage: move
    final Set<Op> initialOps = etPlan.getInitialOps();
    assertEquals(1, initialOps.size());
    final Op initialOp = initialOps.iterator().next();
    assertEquals(Op.OpType.MOVE, initialOp.getOpType());

    // second stage: unassociate
    final Set<Op> secondOps = etPlan.onComplete(initialOp);
    assertEquals(1, secondOps.size());
    final Op secondOp = secondOps.iterator().next();
    assertEquals(Op.OpType.UNASSOCIATE, secondOp.getOpType());

    // third stage: deallocate
    final Set<Op> thirdOps = etPlan.onComplete(secondOp);
    assertEquals(1, thirdOps.size());
    final Op thirdOp = thirdOps.iterator().next();
    assertEquals(Op.OpType.DEALLOCATE, thirdOp.getOpType());

    // fourth stage: allocate
    final Set<Op> fourthOps = etPlan.onComplete(thirdOp);
    assertEquals(1, fourthOps.size());
    final Op fourthOp = fourthOps.iterator().next();
    assertEquals(Op.OpType.ALLOCATE, fourthOp.getOpType());

    // fifth stage: subscribe and associate
    final Set<Op> fifthOps = etPlan.onComplete(fourthOp);
    assertEquals(2, fifthOps.size());
    final Iterator<Op> iter = fifthOps.iterator();
    final Op fifthOp0 = iter.next();
    final Op fifthOp1 = iter.next();

    // one operation should be SUBSCRIBE and the other should be ASSOCIATE
    assertTrue(fifthOp0.getOpType().equals(Op.OpType.SUBSCRIBE) || fifthOp0.getOpType().equals(Op.OpType.ASSOCIATE));
    assertTrue(fifthOp1.getOpType().equals(Op.OpType.SUBSCRIBE) || fifthOp1.getOpType().equals(Op.OpType.ASSOCIATE));
    assertNotEquals(fifthOp0.getOpType(), fifthOp1.getOpType());

    final Op subscribeOp = fifthOp0.getOpType().equals(Op.OpType.SUBSCRIBE) ? fifthOp0 : fifthOp1;
    final Op associateOp = fifthOp0.getOpType().equals(Op.OpType.ASSOCIATE) ? fifthOp0 : fifthOp1;

    // sixth stage: move
    final Set<Op> sixthOps = etPlan.onComplete(associateOp);
    assertEquals(1, sixthOps.size());
    final Op sixthOp = sixthOps.iterator().next();
    assertEquals(Op.OpType.MOVE, sixthOp.getOpType());

    assertTrue(etPlan.onComplete(sixthOp).isEmpty());

    // last stage: start
    final Set<Op> finalOps = etPlan.onComplete(subscribeOp);
    assertEquals(1, finalOps.size());
    final Op finalOp = finalOps.iterator().next();
    assertEquals(Op.OpType.START, finalOp.getOpType());

    assertTrue(etPlan.onComplete(finalOp).isEmpty());
  }
}
