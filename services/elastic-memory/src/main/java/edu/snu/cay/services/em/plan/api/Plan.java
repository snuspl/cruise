/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.services.em.plan.api;

import edu.snu.cay.services.em.plan.impl.EMOperation;

import java.util.Collection;
import java.util.Set;

/**
 * A plan to be executed by {@link PlanExecutor}.
 * It also embeds the dependency information between detailed steps, {@link EMOperation)s.
 * {@link PlanExecutor} can execute the plan by following steps.
 *   1. At first, call {@link #getNextOperationsToExecute()} to obtain operations to execute.
 *   2. When the operation is completed, call {@link #markEMOperationComplete(EMOperation)} to mark it as completed
 *    and obtain a set of operations enabled by the completion of the operation.
 *   2-1. Start executing the obtained operations.
 *   2-2. If step 2 returns an empty set, check whether the whole plan is completed,
 *    using {@link #getNextOperationsToExecute()}.
 *   3. Wait the completion of operations. Goto step 2 again.
 */
public interface Plan {
  /**
   * Gets ready operations that have no prerequisite operation.
   * @return a set of ready operations
   */
  Set<EMOperation> getNextOperationsToExecute();

  /**
   * Marks the operation complete.
   * It returns operations that become ready at the completion of the operation.
   * @param operation the completed operation
   * @return a set of operations that become ready
   */
  Set<EMOperation> markEMOperationComplete(EMOperation operation);

  /**
   * Evaluators to be added.
   * @return IDs of evaluators to add. These IDs are referenced by the transfer steps in this plan.
   *     Different evaluator IDs will likely be assigned during plan execution.
   */
  Collection<String> getEvaluatorsToAdd(String namespace);

  /**
   * Evaluators to be deleted.
   * @return IDs of evaluators to delete
   */
  Collection<String> getEvaluatorsToDelete(String namespace);

  /**
   * Transfer steps to be applied.
   * @return src, dst, and information about data to be transferred
   */
  Collection<TransferStep> getTransferSteps(String namespace);
}
