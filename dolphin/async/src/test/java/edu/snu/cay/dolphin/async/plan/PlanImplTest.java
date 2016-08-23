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
package edu.snu.cay.dolphin.async.plan;

import edu.snu.cay.dolphin.async.optimizer.parameters.Constants;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.PlanOperation;
import edu.snu.cay.services.em.plan.api.TransferStep;
import edu.snu.cay.services.em.plan.impl.EMPlanOperation;
import edu.snu.cay.services.em.plan.impl.TransferStepImpl;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Test for PlanImpl class.
 * It tests the correctness of Plan building including a DAG that represents execution dependency between steps.
 */
public final class PlanImplTest {
  private static final String NAMESPACE_PREFIX = "NAMESPACE";
  private static final String EVAL_PREFIX = "EVAL";

  /**
   * Tests for building an arbitrary plan, a combination of multiple Add, Del, and Move operations.
   * While building a plan, a builder will automatically adds Start and Stop for Add and Del in worker-side evaluators.
   * The generated plan adds and deletes multiple distinct evaluators.
   * For testing move steps, it moves data from evaluators being deleted to a single source evaluator
   * and moves data from a single destination evaluator to evaluators being added.
   */
  @Test
  public void testPlanBuilding() {
    final int numEvalsToAdd = 50;
    final int numEvalsToDel = 50;
    final int numTransfers = numEvalsToAdd + numEvalsToDel;

    final Set<String> namespaces = new HashSet<>();
    final List<String> evalsToAdd = new ArrayList<>(numEvalsToAdd);
    final List<String> evalsToDel = new ArrayList<>(numEvalsToDel);
    final List<TransferStep> transferSteps = new ArrayList<>(numTransfers);

    namespaces.add(Constants.NAMESPACE_SERVER);
    namespaces.add(Constants.NAMESPACE_WORKER);

    int evalIdCounter = 0;

    for (int i = 0; i < numEvalsToAdd; i++) {
      evalsToAdd.add(EVAL_PREFIX + evalIdCounter++);
    }
    for (int i = 0; i < numEvalsToDel; i++) {
      evalsToDel.add(EVAL_PREFIX + evalIdCounter++);
    }

    for (final String evalToAdd : evalsToAdd) {
      transferSteps.add(new TransferStepImpl(EVAL_PREFIX + "-SRC", evalToAdd, new DataInfoImpl(1)));
    }
    for (final String evalToDel : evalsToDel) {
      transferSteps.add(new TransferStepImpl(evalToDel, EVAL_PREFIX + "-DEST", new DataInfoImpl(1)));
    }

    final PlanImpl.Builder planBuilder = PlanImpl.newBuilder();
    for (final String namespace : namespaces) {
      planBuilder
          .addEvaluatorsToAdd(namespace, evalsToAdd)
          .addEvaluatorsToDelete(namespace, evalsToDel)
          .addTransferSteps(namespace, transferSteps)
          .build();
    }

    final Plan plan = planBuilder.build();

    // Start/Stop is generated for only Worker namespace
    final int numWorkersToStart = numEvalsToAdd;
    final int numWorkersToStop = numEvalsToDel;
    final int numServersToSync = numEvalsToDel;

    // test whether a plan contains all the explicitly added steps and automatically generated start/stop/sync steps
    assertEquals("Plan does not contain all the steps that we expect",
        (numEvalsToAdd + numEvalsToDel + numTransfers) * 2 + (numWorkersToStart + numWorkersToStop + numServersToSync),
        plan.getPlanSize());

    for (final String namespace : namespaces) {
      final Collection<String> addSteps = plan.getEvaluatorsToAdd(namespace);
      assertTrue("Plan does not contain all Add steps that we expect", addSteps.containsAll(evalsToAdd));
      assertEquals("Plan contains additional Add steps that we do not expect", evalsToAdd.size(), addSteps.size());

      final Collection<String> delSteps = plan.getEvaluatorsToDelete(namespace);
      assertTrue("Plan does not contain all Delete steps that we expect", delSteps.containsAll(evalsToDel));
      assertEquals("Plan contains additional Delete steps that we do not expect", evalsToDel.size(), delSteps.size());

      final Collection<TransferStep> moveSteps = plan.getTransferSteps(namespace);
      assertTrue("Plan does not contain all Move steps that we expect", moveSteps.containsAll(transferSteps));
      assertEquals("Plan contains additional Move steps that we do not expect", transferSteps.size(), moveSteps.size());
    }
  }

  /**
   * Tests whether the plan builder detects a violation in the plans.
   */
  // TODO #725: remove duplicate code with the default one of EM
  @Test
  public void testInvalidPlans() {
    PlanImpl.Builder planBuilder;

    // 1. case of moving out data from newly added eval
    planBuilder = PlanImpl.newBuilder()
        .addEvaluatorToAdd(NAMESPACE_PREFIX, EVAL_PREFIX + 0)
        .addTransferStep(NAMESPACE_PREFIX,
            new TransferStepImpl(EVAL_PREFIX + 0, EVAL_PREFIX, new DataInfoImpl(1)));

    try {
      planBuilder.build();
      fail("Plan builder fails to detect a violation in the plan");
    } catch (final RuntimeException e) {
      // expected exception
    }

    // 2. case of moving in data to an eval that will be deleted within the same plan
    planBuilder = PlanImpl.newBuilder()
        .addEvaluatorToDelete(NAMESPACE_PREFIX, EVAL_PREFIX + 0)
        .addTransferStep(NAMESPACE_PREFIX,
            new TransferStepImpl(EVAL_PREFIX, EVAL_PREFIX + 0, new DataInfoImpl(1)));

    try {
      planBuilder.build();
      fail("Plan builder fails to detect a violation in the plan");
    } catch (final RuntimeException e) {
      // expected exception
    }

    // 3. case of adding same evaluator both to Add and Del
    planBuilder = PlanImpl.newBuilder()
        .addEvaluatorToAdd(NAMESPACE_PREFIX, EVAL_PREFIX + 0)
        .addEvaluatorToDelete(NAMESPACE_PREFIX, EVAL_PREFIX + 0);

    try {
      planBuilder.build();
      fail("Plan builder fails to detect a violation in the plan");
    } catch (final RuntimeException e) {
      // expected exception
    }

    // 4. case of plan with a cyclic dependency (Add -> Move -> Del -> Add)
    planBuilder = PlanImpl.newBuilder()
        .setNumAvailableExtraEvaluators(0)
        .addEvaluatorToAdd(NAMESPACE_PREFIX, EVAL_PREFIX + 0)
        .addEvaluatorToDelete(NAMESPACE_PREFIX, EVAL_PREFIX + 1)
        .addTransferStep(NAMESPACE_PREFIX,
            new TransferStepImpl(EVAL_PREFIX + 1, EVAL_PREFIX + 0, new DataInfoImpl(1)));

    try {
      planBuilder.build();
      fail("Plan builder fails to detect a violation in the plan");
    } catch (final RuntimeException e) {
      // expected exception
    }
  }

  /**
   * Simply executes {@code prevOpsToExec} without no validation,
   * and returns next operations to execute.
   */
  private Set<PlanOperation> executeOperations(final Set<PlanOperation> prevOpsToExec,
                                               final Plan plan,
                                               final AtomicInteger numExecutedOps) {
    final Set<PlanOperation> nextOpsToExec = new HashSet<>();

    for (final PlanOperation operation : prevOpsToExec) {
      numExecutedOps.incrementAndGet();
      final Set<PlanOperation> followingOps = plan.onComplete(operation);

      nextOpsToExec.addAll(followingOps);
    }

    return nextOpsToExec;
  }

  /**
   * Executes {@code prevOpsToExec}, validating that
   * each executed operation enables one operation for next execution,
   * and returns next operations to execute.
   */
  private Set<PlanOperation> testOneToOneDependency(final Set<PlanOperation> prevOpsToExec,
                                                    final Plan plan,
                                                    final AtomicInteger numExecutedOps) {
    final Set<PlanOperation> nextOpsToExec = new HashSet<>();

    for (final PlanOperation operation : prevOpsToExec) {
      numExecutedOps.incrementAndGet();
      final Set<PlanOperation> followingOps = plan.onComplete(operation);
      assertEquals(1, followingOps.size());

      nextOpsToExec.addAll(followingOps);
    }

    assertEquals(prevOpsToExec.size(), nextOpsToExec.size());

    return nextOpsToExec;
  }

  /**
   * Executes {@code prevOpsToExec}, validating that
   * each executed operation enables two operations for next execution,
   * and returns next operations to execute.
   */
  private Set<PlanOperation> testOneToTwoDependency(final Set<PlanOperation> prevOpsToExec,
                                                    final Plan plan,
                                                    final AtomicInteger numExecutedOps) {
    final Set<PlanOperation> nextOpsToExec = new HashSet<>();

    for (final PlanOperation operation : prevOpsToExec) {
      numExecutedOps.incrementAndGet();
      final Set<PlanOperation> followingOps = plan.onComplete(operation);
      assertEquals(2, followingOps.size());

      nextOpsToExec.addAll(followingOps);
    }

    assertEquals(prevOpsToExec.size() * 2, nextOpsToExec.size());

    return nextOpsToExec;
  }

  /**
   * Executes {@code prevOpsToExec}, validating that
   * a pair of executed operation enables one operation for next execution,
   * and returns next operations to execute.
   * It assumes that {@code prevOpsToExec} has only two elements.
   */
  private Set<PlanOperation> testTwoToOneDependency(final Set<PlanOperation> prevOpsToExec,
                                                    final Plan plan,
                                                    final AtomicInteger numExecutedOps) {
    final Set<PlanOperation> nextOpsToExec = new HashSet<>();

    int idx = 0;
    assertEquals("This function assumes that the size of a given op set is 2.", 2, prevOpsToExec.size());

    // a single Del step can be executed after completing each Stop step
    for (final PlanOperation operation : prevOpsToExec) {
      numExecutedOps.incrementAndGet();
      final Set<PlanOperation> followingOps = plan.onComplete(operation);
      if (idx++ == 0) {
        assertTrue(followingOps.isEmpty());
      } else {
        assertEquals(1, followingOps.size());
        nextOpsToExec.addAll(followingOps);
      }
    }

    assertEquals(prevOpsToExec.size() / 2, nextOpsToExec.size());

    return nextOpsToExec;
  }

  /**
   * Tests a generated plan contains the correct dependency information
   * that each Delete is executed before a single Add, and each server-side Delete is preceded by a Sync
   * and each worker-side Add is followed by a Start.
   */
  @Test
  public void testSyncDelAddStartPlanDependency() {
    // sync -> del -> add -> start
    final Plan plan = PlanImpl.newBuilder()
        .setNumAvailableExtraEvaluators(0)
        .addEvaluatorToAdd(Constants.NAMESPACE_WORKER, EVAL_PREFIX + 0) // Adding worker involves Start
        .addEvaluatorToDelete(Constants.NAMESPACE_SERVER, EVAL_PREFIX + 1)
        .addEvaluatorToDelete(Constants.NAMESPACE_SERVER, EVAL_PREFIX + 2)
        .addEvaluatorToAdd(Constants.NAMESPACE_WORKER, EVAL_PREFIX + 3) // Adding worker involves Start
        .build();

    final AtomicInteger numExecutedOps = new AtomicInteger(0); // increase 1 on every plan.onComplete()

    final Set<PlanOperation> firstOpsToExec = plan.getInitialOps();
    assertEquals(2, firstOpsToExec.size());
    for (final PlanOperation operation : firstOpsToExec) {
      assertEquals(DolphinPlanOperation.SYNC_OP, operation.getOpType());
    }

    Set<PlanOperation> nextOpsToExecute;

    nextOpsToExecute = testOneToOneDependency(firstOpsToExec, plan, numExecutedOps);
    for (final PlanOperation nextOp : nextOpsToExecute) {
      assertEquals(EMPlanOperation.DEL_OP, nextOp.getOpType());
    }

    nextOpsToExecute = testOneToOneDependency(nextOpsToExecute, plan, numExecutedOps);
    for (final PlanOperation nextOp : nextOpsToExecute) {
      assertEquals(EMPlanOperation.ADD_OP, nextOp.getOpType());
    }

    nextOpsToExecute = testOneToOneDependency(nextOpsToExecute, plan, numExecutedOps);
    for (final PlanOperation nextOp : nextOpsToExecute) {
      assertEquals(DolphinPlanOperation.START_OP, nextOp.getOpType());
    }

    nextOpsToExecute = executeOperations(nextOpsToExecute, plan, numExecutedOps);
    assertTrue(nextOpsToExecute.isEmpty());

    assertEquals(numExecutedOps.get(), plan.getPlanSize());
  }

  /**
   * Tests a generated plan contains the correct dependency information
   * that each Delete is executed before a single Add, and each worker-side Del is preceded by a Stop.
   */
  @Test
  public void testStopDelAddPlanDependency() {
    // stop -> del -> add
    final Plan plan = PlanImpl.newBuilder()
        .setNumAvailableExtraEvaluators(0)
        .addEvaluatorToAdd(Constants.NAMESPACE_SERVER, EVAL_PREFIX + 0)
        .addEvaluatorToDelete(Constants.NAMESPACE_WORKER, EVAL_PREFIX + 1) // Deleting worker involves Stop
        .addEvaluatorToDelete(Constants.NAMESPACE_WORKER, EVAL_PREFIX + 2) // Deleting worker involves Stop
        .addEvaluatorToAdd(Constants.NAMESPACE_SERVER, EVAL_PREFIX + 3)
        .build();

    final AtomicInteger numExecutedOps = new AtomicInteger(0); // increase 1 on every plan.onComplete()

    final Set<PlanOperation> firstOpsToExec = plan.getInitialOps();
    assertEquals(2, firstOpsToExec.size());
    for (final PlanOperation operation : firstOpsToExec) {
      assertEquals(DolphinPlanOperation.STOP_OP, operation.getOpType());
    }

    Set<PlanOperation> nextOpsToExecute;

    nextOpsToExecute = testOneToOneDependency(firstOpsToExec, plan, numExecutedOps);
    for (final PlanOperation nextOp : nextOpsToExecute) {
      assertEquals(EMPlanOperation.DEL_OP, nextOp.getOpType());
    }

    nextOpsToExecute = testOneToOneDependency(nextOpsToExecute, plan, numExecutedOps);
    for (final PlanOperation nextOp : nextOpsToExecute) {
      assertEquals(EMPlanOperation.ADD_OP, nextOp.getOpType());
    }

    nextOpsToExecute = executeOperations(nextOpsToExecute, plan, numExecutedOps);
    assertTrue(nextOpsToExecute.isEmpty());

    assertEquals(numExecutedOps.get(), plan.getPlanSize());
  }

  /**
   * Tests a generated plan contains the correct dependency information
   * that Sync is placed between Move and Del,
   * when the source evaluator of Move is same with the target eval of Del step, and the eval is server.
   */
  @Test
  public void testMoveSyncDelPlanDependency() {
    // move -> sync -> del
    final Plan plan = PlanImpl.newBuilder()
        .addEvaluatorToDelete(Constants.NAMESPACE_SERVER, EVAL_PREFIX + 0)
        .addTransferStep(Constants.NAMESPACE_SERVER,
            new TransferStepImpl(EVAL_PREFIX + 0, EVAL_PREFIX + 1, new DataInfoImpl(1)))
        .addTransferStep(Constants.NAMESPACE_SERVER,
            new TransferStepImpl(EVAL_PREFIX + 0, EVAL_PREFIX + 2, new DataInfoImpl(1)))
        .build();

    final AtomicInteger numExecutedOps = new AtomicInteger(0); // increase 1 on every plan.onComplete()

    // Stops should be executed first
    final Set<PlanOperation> firstOpsToExec = plan.getInitialOps();
    assertEquals(2, firstOpsToExec.size());
    for (final PlanOperation operation : firstOpsToExec) {
      assertEquals(EMPlanOperation.MOVE_OP, operation.getOpType());
    }

    Set<PlanOperation> nextOpsToExecute;

    nextOpsToExecute = testTwoToOneDependency(firstOpsToExec, plan, numExecutedOps);
    for (final PlanOperation nextOp : nextOpsToExecute) {
      assertEquals(DolphinPlanOperation.SYNC_OP, nextOp.getOpType());
    }

    nextOpsToExecute = testOneToOneDependency(nextOpsToExecute, plan, numExecutedOps);
    for (final PlanOperation nextOp : nextOpsToExecute) {
      assertEquals(EMPlanOperation.DEL_OP, nextOp.getOpType());
    }

    nextOpsToExecute = executeOperations(nextOpsToExecute, plan, numExecutedOps);
    assertTrue(nextOpsToExecute.isEmpty());

    assertEquals(numExecutedOps.get(), plan.getPlanSize());
  }

  /**
   * Tests a generated plan contains the correct dependency information
   * that Stops are executed first, followed by Moves and then Dels are executed at the end
   * when the source evaluator of Move is same with the target eval of Del step, and the eval is worker.
   */
  @Test
  public void testStopMoveDelPlanDependency() {
    // stop -> move -> del
    final Plan plan = PlanImpl.newBuilder()
        // the first set of dependencies comprised of one Delete and two moves
        .addEvaluatorToDelete(Constants.NAMESPACE_WORKER, EVAL_PREFIX + 0)
        .addTransferStep(Constants.NAMESPACE_WORKER,
            new TransferStepImpl(EVAL_PREFIX + 0, EVAL_PREFIX + 1, new DataInfoImpl(1)))
        .addTransferStep(Constants.NAMESPACE_WORKER,
            new TransferStepImpl(EVAL_PREFIX + 0, EVAL_PREFIX + 2, new DataInfoImpl(1)))

        // the second set of dependencies comprised of one Delete and two moves
        .addEvaluatorToDelete(Constants.NAMESPACE_WORKER, EVAL_PREFIX + 3)
        .addTransferStep(Constants.NAMESPACE_WORKER,
            new TransferStepImpl(EVAL_PREFIX + 3, EVAL_PREFIX + 1, new DataInfoImpl(1)))
        .addTransferStep(Constants.NAMESPACE_WORKER,
            new TransferStepImpl(EVAL_PREFIX + 3, EVAL_PREFIX + 2, new DataInfoImpl(1)))
        .build();

    final AtomicInteger numExecutedOps = new AtomicInteger(0); // increase 1 on every plan.onComplete()

    // Stops should be executed first
    final Set<PlanOperation> firstOpsToExec = plan.getInitialOps();
    assertEquals(2, firstOpsToExec.size());
    for (final PlanOperation operation : firstOpsToExec) {
      assertEquals(DolphinPlanOperation.STOP_OP, operation.getOpType());
    }

    Set<PlanOperation> nextOpsToExecute;

    for (final PlanOperation stopOp : firstOpsToExec) {
      final Set<PlanOperation> stopOpSet = new HashSet<>();
      stopOpSet.add(stopOp);

      nextOpsToExecute = testOneToTwoDependency(stopOpSet, plan, numExecutedOps);
      for (final PlanOperation nextOp : nextOpsToExecute) {
        assertEquals(EMPlanOperation.MOVE_OP, nextOp.getOpType());
      }

      nextOpsToExecute = testTwoToOneDependency(nextOpsToExecute, plan, numExecutedOps);
      for (final PlanOperation nextOp : nextOpsToExecute) {
        assertEquals(EMPlanOperation.DEL_OP, nextOp.getOpType());
      }

      nextOpsToExecute = executeOperations(nextOpsToExecute, plan, numExecutedOps);
      assertTrue(nextOpsToExecute.isEmpty());
    }

    assertEquals(numExecutedOps.get(), plan.getPlanSize());
  }

  /**
   * Tests a generated plan contains the correct dependency information
   * that Moves follow corresponding Adds when the destination evaluator of Move
   * is same with the target eval of Add step. In addition, Starts follow the Moves when the evaluator is worker.
   */
  @Test
  public void testAddMoveStartPlanDependency() {
    // add -> move -> start
    final Plan plan = PlanImpl.newBuilder()
        // the first set of dependencies comprised of one Add and two moves
        .addTransferStep(Constants.NAMESPACE_WORKER,
            new TransferStepImpl(EVAL_PREFIX + 1, EVAL_PREFIX + 0, new DataInfoImpl(1)))
        .addTransferStep(Constants.NAMESPACE_WORKER,
            new TransferStepImpl(EVAL_PREFIX + 2, EVAL_PREFIX + 0, new DataInfoImpl(1)))
        .addEvaluatorToAdd(Constants.NAMESPACE_WORKER, EVAL_PREFIX + 0)

        // the second set of dependencies comprised of one Add and two moves
        .addTransferStep(Constants.NAMESPACE_WORKER,
            new TransferStepImpl(EVAL_PREFIX + 1, EVAL_PREFIX + 3, new DataInfoImpl(1)))
        .addTransferStep(Constants.NAMESPACE_WORKER,
            new TransferStepImpl(EVAL_PREFIX + 2, EVAL_PREFIX + 3, new DataInfoImpl(1)))
        .addEvaluatorToAdd(Constants.NAMESPACE_WORKER, EVAL_PREFIX + 3)
        .build();

    final AtomicInteger numExecutedOps = new AtomicInteger(0); // increase 1 on every plan.onComplete()

    // Adds should be executed first
    final Set<PlanOperation> firstOpsToExec = plan.getInitialOps();
    assertEquals(2, firstOpsToExec.size());
    for (final PlanOperation operation : firstOpsToExec) {
      assertEquals(EMPlanOperation.ADD_OP, operation.getOpType());
    }

    Set<PlanOperation> nextOpsToExecute;

    for (final PlanOperation addOp : firstOpsToExec) {
      final Set<PlanOperation> addOpSet = new HashSet<>();
      addOpSet.add(addOp);

      nextOpsToExecute = testOneToTwoDependency(addOpSet, plan, numExecutedOps);
      for (final PlanOperation operation : nextOpsToExecute) {
        assertEquals(EMPlanOperation.MOVE_OP, operation.getOpType());
      }

      nextOpsToExecute = testTwoToOneDependency(nextOpsToExecute, plan, numExecutedOps);
      for (final PlanOperation operation : nextOpsToExecute) {
        assertEquals(DolphinPlanOperation.START_OP, operation.getOpType());
      }

      nextOpsToExecute = executeOperations(nextOpsToExecute, plan, numExecutedOps);
      assertTrue(nextOpsToExecute.isEmpty());
    }

    assertEquals(numExecutedOps.get(), plan.getPlanSize());
  }
}
