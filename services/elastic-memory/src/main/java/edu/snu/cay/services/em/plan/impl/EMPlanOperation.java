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

import edu.snu.cay.services.em.plan.api.PlanOperation;
import edu.snu.cay.services.em.plan.api.TransferStep;
import org.apache.reef.util.Optional;

/**
 * A class representing EM's plan operation.
 */
public final class EMPlanOperation implements PlanOperation {
  public static final String ADD_OP = "ADD";
  public static final String DEL_OP = "DEL";
  public static final String MOVE_OP = "MOVE";

  private final String namespace;
  private final String opType;
  private final String evalId;
  private final Optional<TransferStep> transferStep;

  /**
   * A constructor for ADD and DELETE operations.
   * @param namespace a namespace of operation
   * @param opType a type of operation, which is one of ADD or REMOVE
   * @param evalId a target evaluator id
   */
  public EMPlanOperation(final String namespace, final String opType, final String evalId) {
    this.namespace = namespace;
    this.opType = opType;
    this.evalId = evalId;
    this.transferStep = Optional.empty();
  }

  /**
   * A constructor for MOVE operation.
   * @param namespace a namespace of operation
   * @param transferStep a TransferStep including src, dest, data info of MOVE operation
   */
  public EMPlanOperation(final String namespace, final TransferStep transferStep) {
    this.namespace = namespace;
    this.opType = MOVE_OP;
    this.evalId = transferStep.getSrcId();
    this.transferStep = Optional.of(transferStep);
  }

  /**
   * @return a namespace of operation
   */
  public String getNamespace() {
    return namespace;
  }

  /**
   * @return a type of operation
   */
  public String getOpType() {
    return opType;
  }

  /**
   * @return a target evaluator id if the operation type is ADD or DELETE.
   * For Move operation, it returns a source evaluator id.
   */
  public String getEvalId() {
    return evalId;
  }

  /**
   * @return an Optional with the TransferStep if the operation type is MOVE.
   * For ADD or DELETE operations, it returns an empty Optional.
   */
  public Optional<TransferStep> getTransferStep() {
    return transferStep;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    final EMPlanOperation other = (EMPlanOperation) obj;

    return namespace.equals(other.namespace) &&
        opType.equals(other.opType) &&
        evalId.equals(other.evalId) &&
        transferStep.equals(other.transferStep);
  }

  @Override
  public int hashCode() {
    int result = namespace.hashCode();
    result = 31 * result + opType.hashCode();
    result = 31 * result + evalId.hashCode();
    result = 31 * result + transferStep.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "EMPlanOperation{" +
        "namespace='" + namespace + '\'' +
        ", opType=" + opType +
        ", evalId=" + evalId +
        ", transferStep=" + transferStep +
        '}';
  }
}
