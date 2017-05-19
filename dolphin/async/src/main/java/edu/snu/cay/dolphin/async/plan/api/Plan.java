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
package edu.snu.cay.dolphin.async.plan.api;

import java.util.Collection;

/**
 * A plan to be executed by {@link PlanExecutor}.
 * It also embeds the dependency information between detailed steps, {@link PlanOperation )s.
 * {@link PlanExecutor} can execute the plan by following steps.
 *   1. At first, call {@link #getInitialOps()} to obtain operations to execute.
 *   2. When the operation is completed, call {@link #onComplete(PlanOperation)} to mark it as completed
 *    and obtain a set of operations enabled by the completion of the operation.
 *   2-1. Start executing the obtained operations.
 *   2-2. If step 2 returns an empty set, check whether the whole plan is completed,
 *    using {@link #getPlanSize()}.
 *   3. Wait the completion of operations. Goto step 2 again.
 */
public interface Plan {

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
