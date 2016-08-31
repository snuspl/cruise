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

import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.PlanOperation;
import edu.snu.cay.services.em.plan.api.TransferStep;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A plan implementation that supports EM's default plan operations and Dolphin-specific plan operations.
 * The builder checks the feasibility of plan and dependencies between detailed steps.
 */
public final class EmptyPlan implements Plan {
  public EmptyPlan() {
  }

  @Override
  public int getPlanSize() {
    return 0;
  }

  @Override
  public synchronized Set<PlanOperation> getInitialOps() {
    return new HashSet<>();
  }

  @Override
  public synchronized Set<PlanOperation> onComplete(final PlanOperation operation) {
    return new HashSet<>();
  }

  @Override
  public Collection<String> getEvaluatorsToAdd(final String namespace) {
    return new HashSet<>();
  }

  @Override
  public Collection<String> getEvaluatorsToDelete(final String namespace) {
    return new HashSet<>();
  }

  @Override
  public Collection<TransferStep> getTransferSteps(final String namespace) {
    return new HashSet<>();
  }

  @Override
  public String toString() {
    return "EmptyPlan";
  }

}
