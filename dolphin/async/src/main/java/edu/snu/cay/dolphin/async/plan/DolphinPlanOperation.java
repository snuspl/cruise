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

import edu.snu.cay.services.em.plan.api.PlanOperation;
import edu.snu.cay.services.em.plan.api.TransferStep;
import org.apache.reef.util.Optional;

/**
 * A class representing Dolphin-specific plan operation.
 * Regarding to EM's ADD/DEL, Dolphin's worker tasks should be controlled
 * to prevent them from running task iterations with empty dataset.
 */
public final class DolphinPlanOperation implements PlanOperation {
  static final String START_OP = "START"; // start worker task
  static final String STOP_OP = "STOP"; // stop worker task

  private final String namespace;
  private final String opType;
  private final String evalId;

  /**
   * A constructor for START and STOP operations.
   * @param namespace a namespace of operation
   * @param opType a type of operation
   * @param evalId a target evaluator id
   */
  public DolphinPlanOperation(final String namespace, final String opType, final String evalId) {
    this.namespace = namespace;
    this.opType = opType;
    this.evalId = evalId;
  }

  @Override
  public String getNamespace() {
    return namespace;
  }

  @Override
  public String getOpType() {
    return opType;
  }

  @Override
  public String getEvalId() {
    return evalId;
  }

  @Override
  public Optional<TransferStep> getTransferStep() {
    return Optional.empty();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    final DolphinPlanOperation other = (DolphinPlanOperation) obj;

    return namespace.equals(other.namespace) &&
        opType.equals(other.opType) &&
        evalId.equals(other.evalId);
  }

  @Override
  public int hashCode() {
    int result = namespace.hashCode();
    result = 31 * result + opType.hashCode();
    result = 31 * result + evalId.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "DolphinPlanOperation{" +
        "namespace='" + namespace + '\'' +
        ", opType=" + opType +
        ", evalId=" + evalId +
        '}';
  }
}
