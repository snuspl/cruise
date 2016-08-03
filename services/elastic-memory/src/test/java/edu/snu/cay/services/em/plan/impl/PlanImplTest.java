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
package edu.snu.cay.services.em.plan.impl;

import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.PlanOperation;
import edu.snu.cay.services.em.plan.api.TransferStep;
import org.junit.Test;

import java.util.*;

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
   * The generated plan adds and deletes multiple distinct evaluators.
   * For testing move steps, it moves data from evaluators being deleted to a single source evaluator
   * and moves data from a single destination evaluator to evaluators being added.
   */
  @Test
  public void testPlanBuilding() {
    final int numNameSpaces = 2;
    final int numEvalsToAdd = 50;
    final int numEvalsToDel = 50;
    final int numTransfers = numEvalsToAdd + numEvalsToDel;

    final Set<String> namespaces = new HashSet<>(numNameSpaces);
    final List<String> evalsToAdd = new ArrayList<>(numEvalsToAdd);
    final List<String> evalsToDel = new ArrayList<>(numEvalsToDel);
    final List<TransferStep> transferSteps = new ArrayList<>(numTransfers);

    for (int i = 0; i < numNameSpaces; i++) {
      namespaces.add(NAMESPACE_PREFIX + i);
    }

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

    // test whether a plan contains all the added steps
    assertEquals("Plan does not contain all the steps that we expect",
        (numEvalsToAdd + numEvalsToDel + numTransfers) * numNameSpaces, plan.getPlanSize());

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
        .setNumExtraEvaluators(0)
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
   * Tests a generated plan contains the correct dependency information
   * that each Delete is executed before a single Add.
   */
  @Test
  public void testDelAddPlanDependency() {
    // del -> add
    final Plan plan = PlanImpl.newBuilder()
        .setNumExtraEvaluators(0)
        .addEvaluatorToAdd(NAMESPACE_PREFIX, EVAL_PREFIX + 0)
        .addEvaluatorToDelete(NAMESPACE_PREFIX, EVAL_PREFIX + 1)
        .addEvaluatorToDelete(NAMESPACE_PREFIX, EVAL_PREFIX + 2)
        .addEvaluatorToAdd(NAMESPACE_PREFIX, EVAL_PREFIX + 3)
        .build();

    // Dels should be executed first to make a room for Adds
    final Set<PlanOperation> firstOpsToExec = plan.getReadyOps();
    assertEquals(2, firstOpsToExec.size());
    for (final PlanOperation operation : firstOpsToExec) {
      assertEquals(EMPlanOperation.OpType.DEL, ((EMPlanOperation) operation).getOpType());
    }

    final Set<EMPlanOperation> executingPlans = new HashSet<>();

    // a single Add step can be executed after completing each Del step

    for (final PlanOperation operation : firstOpsToExec) {
      final Set<PlanOperation> nextOpsToExec = plan.onComplete(operation);
      assertEquals(1, nextOpsToExec.size());
      final PlanOperation nextOpToExec = nextOpsToExec.iterator().next();

      assertEquals(EMPlanOperation.OpType.ADD, ((EMPlanOperation) nextOpToExec).getOpType());

      executingPlans.add((EMPlanOperation) nextOpToExec);
    }

    // as a result of two Dels, two Adds started
    assertEquals(2, executingPlans.size());

    // these two Adds are the final stages of the plan
    for (final EMPlanOperation executingPlan : executingPlans) {
      final Set<PlanOperation> nextOpsToExec = plan.onComplete(executingPlan);
      assertTrue(nextOpsToExec.isEmpty());
    }

    assertTrue(plan.getReadyOps().isEmpty());
  }

  /**
   * Tests a generated plan contains the correct dependency information
   * that Moves are executed first, followed by corresponding Dels
   * when the source evaluator of Move is same with the target eval of Del step.
   */
  @Test
  public void testMoveDelPlanDependency() {
    // move -> del
    final Plan plan = PlanImpl.newBuilder()
        // the first set of dependencies comprised of one Delete and two moves
        .addTransferStep(NAMESPACE_PREFIX,
            new TransferStepImpl(EVAL_PREFIX + 0, EVAL_PREFIX + 1, new DataInfoImpl(1)))
        .addEvaluatorToDelete(NAMESPACE_PREFIX, EVAL_PREFIX + 0)
        .addTransferStep(NAMESPACE_PREFIX,
            new TransferStepImpl(EVAL_PREFIX + 0, EVAL_PREFIX + 2, new DataInfoImpl(1)))

        // the second set of dependencies comprised of one Delete and two moves
        .addEvaluatorToDelete(NAMESPACE_PREFIX, EVAL_PREFIX + 3)
        .addTransferStep(NAMESPACE_PREFIX,
            new TransferStepImpl(EVAL_PREFIX + 3, EVAL_PREFIX + 1, new DataInfoImpl(1)))
        .addTransferStep(NAMESPACE_PREFIX,
            new TransferStepImpl(EVAL_PREFIX + 3, EVAL_PREFIX + 2, new DataInfoImpl(1)))
        .build();

    // Moves should be executed first
    final Set<PlanOperation> firstOpsToExec = plan.getReadyOps();
    assertEquals(4, firstOpsToExec.size());

    final Set<EMPlanOperation> firstMoveSet = new HashSet<>();
    final Set<EMPlanOperation> secondMoveSet = new HashSet<>();

    // after finishing Moves from each evaluator, Dels for the evaluator will be ready
    for (final PlanOperation operation : firstOpsToExec) {
      final EMPlanOperation emPlanOp = (EMPlanOperation) operation;
      assertEquals(EMPlanOperation.OpType.MOVE, emPlanOp.getOpType());

      if (emPlanOp.getTransferStep().get().getSrcId().equals(EVAL_PREFIX + 0)) {
        firstMoveSet.add(emPlanOp);
      } else {
        secondMoveSet.add(emPlanOp);
      }
    }
    assertEquals(2, firstMoveSet.size());
    assertEquals(2, secondMoveSet.size());

    // Delete will be ready after finishing all Moves from target evaluator
    final Iterator<EMPlanOperation> firstMoveSetIter = firstMoveSet.iterator();
    assertTrue(plan.onComplete(firstMoveSetIter.next()).isEmpty());
    final Set<PlanOperation> nextOpsToExec = plan.onComplete(firstMoveSetIter.next());
    assertEquals(1, nextOpsToExec.size());

    final Iterator<EMPlanOperation> secondMoveSetIter = secondMoveSet.iterator();
    assertTrue(plan.onComplete(secondMoveSetIter.next()).isEmpty());
    nextOpsToExec.addAll(plan.onComplete(secondMoveSetIter.next()));
    assertEquals(2, nextOpsToExec.size());

    for (final PlanOperation operation : nextOpsToExec) {
      assertEquals(EMPlanOperation.OpType.DEL, ((EMPlanOperation) operation).getOpType());

      // these Deletes are the final stages of the plan
      assertTrue(plan.onComplete(operation).isEmpty());
    }
    assertTrue(plan.getReadyOps().isEmpty());
  }

  /**
   * Tests a generated plan contains the correct dependency information
   * that Moves follow corresponding Adds when the destination evaluator of Move
   * is same with the target eval of Add step.
   */
  @Test
  public void testAddMovePlanDependency() {
    // add -> move
    final Plan plan = PlanImpl.newBuilder()
        // the first set of dependencies comprised of one Add and two moves
        .addTransferStep(NAMESPACE_PREFIX,
            new TransferStepImpl(EVAL_PREFIX + 1, EVAL_PREFIX + 0, new DataInfoImpl(1)))
        .addEvaluatorToAdd(NAMESPACE_PREFIX, EVAL_PREFIX + 0)
        .addTransferStep(NAMESPACE_PREFIX,
            new TransferStepImpl(EVAL_PREFIX + 2, EVAL_PREFIX + 0, new DataInfoImpl(1)))

        // the second set of dependencies comprised of one Add and two moves
        .addEvaluatorToAdd(NAMESPACE_PREFIX, EVAL_PREFIX + 3)
        .addTransferStep(NAMESPACE_PREFIX,
            new TransferStepImpl(EVAL_PREFIX + 1, EVAL_PREFIX + 3, new DataInfoImpl(1)))
        .addTransferStep(NAMESPACE_PREFIX,
            new TransferStepImpl(EVAL_PREFIX + 2, EVAL_PREFIX + 3, new DataInfoImpl(1)))
        .build();

    // Adds should be executed first
    final Set<PlanOperation> firstOpsToExec = plan.getReadyOps();
    assertEquals(2, firstOpsToExec.size());

    EMPlanOperation firstAdd = null;
    EMPlanOperation secondAdd = null;
    for (final PlanOperation operation : firstOpsToExec) {
      final EMPlanOperation emPlanOp = (EMPlanOperation) operation;

      assertEquals(EMPlanOperation.OpType.ADD, emPlanOp.getOpType());

      if (emPlanOp.getEvalId().get().equals(EVAL_PREFIX + 0)) {
        firstAdd = emPlanOp;
      } else {
        secondAdd = emPlanOp;
      }
    }
    assertNotNull(firstAdd);
    assertNotNull(secondAdd);

    // Moves will be ready after finishing Add of destination evaluator
    final Set<PlanOperation> nextOpsToExec = plan.onComplete(firstAdd);
    assertEquals(2, nextOpsToExec.size());

    nextOpsToExec.addAll(plan.onComplete(secondAdd));
    assertEquals(4, nextOpsToExec.size());

    for (final PlanOperation operation : nextOpsToExec) {
      assertEquals(EMPlanOperation.OpType.MOVE, ((EMPlanOperation) operation).getOpType());

      // these Moves are the final stages of the plan
      assertTrue(plan.onComplete(operation).isEmpty());
    }
    assertTrue(plan.getReadyOps().isEmpty());
  }
}
