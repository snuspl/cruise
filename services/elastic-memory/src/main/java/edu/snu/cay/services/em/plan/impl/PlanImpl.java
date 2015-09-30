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
package edu.snu.cay.services.em.plan.impl;

import edu.snu.cay.services.em.plan.api.TransferStep;
import edu.snu.cay.services.em.plan.api.Plan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A plan implementation with builder.
 * The builder checks for duplicate evaluators.
 */
public final class PlanImpl implements Plan {
  private final Collection<String> evaluatorsToAdd;
  private final Collection<String> evaluatorsToDelete;
  private final Collection<TransferStep> transferSteps;

  private PlanImpl(final Collection<String> evaluatorsToAdd,
                   final Collection<String> evaluatorsToDelete,
                   final Collection<TransferStep> transferSteps) {
    this.evaluatorsToAdd = evaluatorsToAdd;
    this.evaluatorsToDelete = evaluatorsToDelete;
    this.transferSteps = transferSteps;
  }

  @Override
  public Collection<String> getEvaluatorsToAdd() {
    return evaluatorsToAdd;
  }

  @Override
  public Collection<String> getEvaluatorsToDelete() {
    return evaluatorsToDelete;
  }

  @Override
  public Collection<TransferStep> getTransferSteps() {
    return transferSteps;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("PlanImpl{");
    sb.append("evaluatorsToAdd=").append(evaluatorsToAdd);
    sb.append(", evaluatorsToDelete=").append(evaluatorsToDelete);
    sb.append(", transferSteps=").append(transferSteps);
    sb.append('}');
    return sb.toString();
  }

  public static PlanImpl.Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder implements org.apache.reef.util.Builder<PlanImpl> {
    private final Set<String> evaluatorsToAdd = new HashSet<>();
    private final Set<String> evaluatorsToDelete = new HashSet<>();
    private final List<TransferStep> transferSteps = new ArrayList<>();

    private Builder() {
    }

    public Builder addEvaluatorToAdd(final String evaluatorId) {
      evaluatorsToAdd.add(evaluatorId);
      return this;
    }

    public Builder addEvaluatorsToAdd(final Collection<String> evaluatorIds) {
      evaluatorsToAdd.addAll(evaluatorIds);
      return this;
    }

    public Builder addEvaluatorToDelete(final String evaluatorId) {
      evaluatorsToDelete.add(evaluatorId);
      return this;
    }

    public Builder addEvaluatorsToDelete(final Collection<String> evaluatorIds) {
      evaluatorsToDelete.addAll(evaluatorIds);
      return this;
    }

    public Builder addTransferStep(final TransferStep transferStep) {
      transferSteps.add(transferStep);
      return this;
    }

    public Builder addTransferSteps(final Collection<TransferStep> newTransferSteps) {
      for (final TransferStep transferStep : newTransferSteps) {
        addTransferStep(transferStep);
      }
      return this;
    }

    @Override
    public PlanImpl build() {
      for (final String evaluator : evaluatorsToAdd) {
        if (evaluatorsToDelete.contains(evaluator)) {
          throw new RuntimeException(evaluator + " is planned for addition and deletion.");
        }
      }

      for (final TransferStep transferStep : transferSteps) {
        if (evaluatorsToDelete.contains(transferStep.getDstId())) {
          throw new RuntimeException(transferStep.getDstId() + " is planned for deletion.");
        } else if (evaluatorsToAdd.contains(transferStep.getSrcId())) {
          throw new RuntimeException(transferStep.getSrcId() + " is planned for addition.");
        }
      }

      return new PlanImpl(evaluatorsToAdd, evaluatorsToDelete, transferSteps);
    }
  }
}
