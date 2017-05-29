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
package edu.snu.cay.dolphin.async.plan.impl;

import edu.snu.cay.dolphin.async.plan.api.PlanDescriptor;
import edu.snu.cay.dolphin.async.plan.api.TransferStep;

import java.util.*;

public final class ILPPlanDescriptor implements PlanDescriptor {
  
  private final Map<String, List<Integer>> evaluatorsToAdd;
  private final Map<String, List<Integer>> evaluatorsToDelete;
  private final Map<String, List<TransferStep>> allTransferSteps;
  
  private ILPPlanDescriptor(final Map<String, List<Integer>> evaluatorsToAdd,
                            final Map<String, List<Integer>> evaluatorsToDelete,
                            final Map<String, List<TransferStep>> allTransferSteps) {
    this.evaluatorsToAdd = evaluatorsToAdd;
    this.evaluatorsToDelete = evaluatorsToDelete;
    this.allTransferSteps = allTransferSteps;
  }
  
  @Override
  public List<Integer> getEvaluatorsToAdd(final String namespace) {
    if (!evaluatorsToAdd.containsKey(namespace)) {
      return Collections.emptyList();
    }
    return evaluatorsToAdd.get(namespace);
  }
  
  @Override
  public List<Integer> getEvaluatorsToDelete(final String namespace) {
    if (!evaluatorsToDelete.containsKey(namespace)) {
      return Collections.emptyList();
    }
    return evaluatorsToDelete.get(namespace);
  }
  
  @Override
  public List<TransferStep> getTransferSteps(final String namespace) {
    if (!allTransferSteps.containsKey(namespace)) {
      return Collections.emptyList();
    }
    return allTransferSteps.get(namespace);
  }
  
  public static ILPPlanDescriptor.Builder newBuilder() {
    return new Builder();
  }
  
  public static final class Builder implements org.apache.reef.util.Builder<PlanDescriptor> {
    private final Map<String, List<Integer>> evaluatorsToAdd = new HashMap<>();
    private final Map<String, List<Integer>> evaluatorsToDelete = new HashMap<>();
    private final Map<String, List<TransferStep>> allTransferSteps = new HashMap<>();
  
    private Builder() {
    }
    
    public Builder addEvaluatorToAdd(final String namespace, final int evaluatorIdx) {
      if (!evaluatorsToAdd.containsKey(namespace)) {
        evaluatorsToAdd.put(namespace, new ArrayList<>());
      }
      evaluatorsToAdd.get(namespace).add(evaluatorIdx);
      return this;
    }
    
    public Builder addEvaluatorToDelete(final String namespace, final int evaluatorIdx) {
      if (!evaluatorsToDelete.containsKey(namespace)) {
        evaluatorsToDelete.put(namespace, new ArrayList<>());
      }
      evaluatorsToDelete.get(namespace).add(evaluatorIdx);
      return this;
    }
  
    public Builder addTransferStep(final String namespace, final TransferStep transferStep) {
      if (!allTransferSteps.containsKey(namespace)) {
        allTransferSteps.put(namespace, new ArrayList<>());
      }
      allTransferSteps.get(namespace).add(transferStep);
      return this;
    }
    
    @Override
    public ILPPlanDescriptor build() {
      return new ILPPlanDescriptor(evaluatorsToAdd, evaluatorsToDelete, allTransferSteps);
    }
  }
}
