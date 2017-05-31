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
package edu.snu.cay.dolphin.async.plan.api;

import edu.snu.cay.dolphin.async.optimizer.impl.ILPPlanGenerator;

import java.util.List;

/**
 * Block moving plan for ILP solver's optimizing solution.
 */
public interface PlanDescriptor {

  /**
   * @return temporal IDs of evaluators to add.
   */
  List<Integer> getEvaluatorsToAdd(String namespace);

  /**
   * @return temporal IDs of evaluators to delete.
   */
  List<Integer> getEvaluatorsToDelete(String namespace);

  /**
   * @return transferring steps that are generated in {@link ILPPlanGenerator}.
   */
  List<TransferStep> getTransferSteps(String namespace);
}
