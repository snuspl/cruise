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

  @Test
  public void testPlanBuilding() {
    final int numNameSpaces = 2;
    final int numEvalsToAdd = 50;
    final int numEvalsToDel = 50;
    final int numTransfers = 50;

    final Set<String> namespaces = new HashSet<>(numNameSpaces);
    final List<String> evalsToAdd = new ArrayList<>(numEvalsToAdd);
    final List<String> evalsToDel = new ArrayList<>(numEvalsToDel);
    final List<TransferStep> transferSteps = new ArrayList<>(numTransfers);

    for (int i = 0; i < numNameSpaces; i++) {
      namespaces.add(NAMESPACE_PREFIX + i);
    }
    for (int i = 0; i < numEvalsToAdd; i++) {
      evalsToAdd.add(EVAL_PREFIX + i);
    }
    for (int i = numEvalsToAdd; i < numEvalsToAdd + numEvalsToDel; i++) {
      evalsToDel.add(EVAL_PREFIX + i);
    }
    for (int i = 0; i < numTransfers; i++) {
      transferSteps.add(new TransferStepImpl(evalsToDel.get(i), evalsToAdd.get(i),
          new DataInfoImpl(1)));
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
    assertEquals("Plan does not contain all the steps what we expect",
        (numEvalsToAdd + numEvalsToDel + numTransfers) * numNameSpaces, plan.getPlanSize());

    for (final String namespace : namespaces) {
      final Collection<String> addSteps = plan.getEvaluatorsToAdd(namespace);
      assertTrue("Plan does not contain all Add steps what we expect", addSteps.containsAll(evalsToAdd));
      assertEquals("Plan contains additional Add steps what we do not expect", evalsToAdd.size(), addSteps.size());

      final Collection<String> delSteps = plan.getEvaluatorsToDelete(namespace);
      assertTrue("Plan does not contain all Delete steps what we expect", delSteps.containsAll(evalsToDel));
      assertEquals("Plan contains additional Delete steps what we do not expect", evalsToDel.size(), delSteps.size());

      final Collection<TransferStep> moveSteps = plan.getTransferSteps(namespace);
      assertTrue("Plan does not contain all Move steps what we expect", moveSteps.containsAll(transferSteps));
      assertEquals("Plan contains additional Move steps what we do not expect", transferSteps.size(), moveSteps.size());
    }
  }

  @Test
  public void testPlanViolationCheck() {
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

    // 2. case of moving in data to eval will be deleted within the same plan
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
  }

  @Test
  public void testDelAddPlanDependency() {
    // del -> add
    final Plan plan = PlanImpl.newBuilder()
        .addEvaluatorToAdd(NAMESPACE_PREFIX, EVAL_PREFIX + 0)
        .addEvaluatorToDelete(NAMESPACE_PREFIX, EVAL_PREFIX + 1)
        .addEvaluatorToDelete(NAMESPACE_PREFIX, EVAL_PREFIX + 2)
        .addEvaluatorToAdd(NAMESPACE_PREFIX, EVAL_PREFIX + 3)
        .build();

    final Set<EMOperation> firstOpsToExec = plan.getReadyOps();
    assertEquals(2, firstOpsToExec.size());
    for (final EMOperation operation : firstOpsToExec) {
      assertEquals(EMOperation.OpType.Del, operation.getOpType());
    }

    final Set<EMOperation> executingPlans = new HashSet<>();

    for (final EMOperation operation : firstOpsToExec) {
      final Set<EMOperation> nextOpsToExec = plan.onComplete(operation);
      assertEquals(1, nextOpsToExec.size());
      final EMOperation nextOpToExec = nextOpsToExec.iterator().next();
      assertEquals(EMOperation.OpType.Add, nextOpToExec.getOpType());

      executingPlans.add(nextOpToExec);
    }

    assertEquals(2, executingPlans.size());
    for (final EMOperation executingPlan : executingPlans) {
      final Set<EMOperation> nextOpsToExec = plan.onComplete(executingPlan);
      assertTrue(nextOpsToExec.isEmpty());
    }

    assertTrue(plan.getReadyOps().isEmpty());
  }

  @Test
  public void testMoveDelPlanDependency() {
    // move -> del
    final Plan plan = PlanImpl.newBuilder()
        // a first set of dependencies comprised of one Delete and two moves
        .addTransferStep(NAMESPACE_PREFIX,
            new TransferStepImpl(EVAL_PREFIX + 0, EVAL_PREFIX + 1, new DataInfoImpl(1)))
        .addEvaluatorToDelete(NAMESPACE_PREFIX, EVAL_PREFIX + 0)
        .addTransferStep(NAMESPACE_PREFIX,
            new TransferStepImpl(EVAL_PREFIX + 0, EVAL_PREFIX + 2, new DataInfoImpl(1)))
        // a second set of dependencies comprised of one Delete and two moves
        .addEvaluatorToDelete(NAMESPACE_PREFIX, EVAL_PREFIX + 3)
        .addTransferStep(NAMESPACE_PREFIX,
            new TransferStepImpl(EVAL_PREFIX + 3, EVAL_PREFIX + 1, new DataInfoImpl(1)))
        .addTransferStep(NAMESPACE_PREFIX,
            new TransferStepImpl(EVAL_PREFIX + 3, EVAL_PREFIX + 2, new DataInfoImpl(1)))
        .build();

    final Set<EMOperation> firstOpsToExec = plan.getReadyOps();
    assertEquals(4, firstOpsToExec.size());

    final Set<EMOperation> firstMoveSet = new HashSet<>();
    final Set<EMOperation> secondMoveSet = new HashSet<>();
    for (final EMOperation operation : firstOpsToExec) {
      assertEquals(EMOperation.OpType.Move, operation.getOpType());

      if (operation.getTransferStep().get().getSrcId().equals(EVAL_PREFIX + 0)) {
        firstMoveSet.add(operation);
      } else {
        secondMoveSet.add(operation);
      }
    }
    assertEquals(2, firstMoveSet.size());
    assertEquals(2, secondMoveSet.size());

    final Iterator<EMOperation> firstMoveSetIter = firstMoveSet.iterator();
    assertTrue(plan.onComplete(firstMoveSetIter.next()).isEmpty());
    final Set<EMOperation> nextOpsToExec = plan.onComplete(firstMoveSetIter.next());
    assertEquals(1, nextOpsToExec.size());

    final Iterator<EMOperation> secondMoveSetIter = secondMoveSet.iterator();
    assertTrue(plan.onComplete(secondMoveSetIter.next()).isEmpty());
    nextOpsToExec.addAll(plan.onComplete(secondMoveSetIter.next()));
    assertEquals(2, nextOpsToExec.size());

    for (final EMOperation operation : nextOpsToExec) {
      assertEquals(EMOperation.OpType.Del, operation.getOpType());

      assertTrue(plan.onComplete(operation).isEmpty());
    }
    assertTrue(plan.getReadyOps().isEmpty());
  }

  @Test
  public void testAddMovePlanDependency() {
    // add -> move
    final Plan plan = PlanImpl.newBuilder()
        // a first set of dependencies comprised of one Add and two moves
        .addTransferStep(NAMESPACE_PREFIX,
            new TransferStepImpl(EVAL_PREFIX + 1, EVAL_PREFIX + 0, new DataInfoImpl(1)))
        .addEvaluatorToAdd(NAMESPACE_PREFIX, EVAL_PREFIX + 0)
        .addTransferStep(NAMESPACE_PREFIX,
            new TransferStepImpl(EVAL_PREFIX + 2, EVAL_PREFIX + 0, new DataInfoImpl(1)))
        // a first set of dependencies comprised of one Add and two moves
        .addEvaluatorToAdd(NAMESPACE_PREFIX, EVAL_PREFIX + 3)
        .addTransferStep(NAMESPACE_PREFIX,
            new TransferStepImpl(EVAL_PREFIX + 1, EVAL_PREFIX + 3, new DataInfoImpl(1)))
        .addTransferStep(NAMESPACE_PREFIX,
            new TransferStepImpl(EVAL_PREFIX + 2, EVAL_PREFIX + 3, new DataInfoImpl(1)))
        .build();

    final Set<EMOperation> firstOpsToExec = plan.getReadyOps();
    assertEquals(2, firstOpsToExec.size());

    EMOperation firstAdd = null;
    EMOperation secondAdd = null;
    for (final EMOperation operation : firstOpsToExec) {
      assertEquals(EMOperation.OpType.Add, operation.getOpType());

      if (operation.getEvalId().get().equals(EVAL_PREFIX + 0)) {
        firstAdd = operation;
      } else {
        secondAdd = operation;
      }
    }
    assertNotNull(firstAdd);
    assertNotNull(secondAdd);

    final Set<EMOperation> nextOpsToExec = plan.onComplete(firstAdd);
    assertEquals(2, nextOpsToExec.size());

    nextOpsToExec.addAll(plan.onComplete(secondAdd));
    assertEquals(4, nextOpsToExec.size());

    for (final EMOperation operation : nextOpsToExec) {
      assertEquals(EMOperation.OpType.Move, operation.getOpType());

      assertTrue(plan.onComplete(operation).isEmpty());
    }
    assertTrue(plan.getReadyOps().isEmpty());
  }
}
