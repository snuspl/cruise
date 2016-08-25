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
public final class EMPlanOperation {
  public static final String ADD_OP = "ADD";
  public static final String DEL_OP = "DEL";
  public static final String MOVE_OP = "MOVE";

  /**
   * Should not be instantiated.
   */
  private EMPlanOperation() {
  }

  public static final class AddPlanOperation implements PlanOperation {
    private final String namespace;
    private final String evalId;
    private final String opType;

    /**
     * A constructor for ADD operation.
     *
     * @param namespace a namespace of operation
     * @param evalId    a target evaluator id
     */
    public AddPlanOperation(final String namespace, final String evalId) {
      this.namespace = namespace;
      this.evalId = evalId;
      this.opType = ADD_OP;
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
    public Optional<String> getEvalId() {
      return Optional.of(evalId);
    }

    @Override
    public Optional<TransferStep> getTransferStep() {
      return Optional.empty();
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final AddPlanOperation that = (AddPlanOperation) o;

      return namespace.equals(that.namespace) && evalId.equals(that.evalId);
    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + evalId.hashCode();
      result = 31 * result + opType.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "AddPlanOperation{" +
          "namespace='" + namespace + '\'' +
          ", evalId='" + evalId + '\'' +
          ", opType='" + opType +'\'' +
          '}';
    }
  }

  public static final class DeletePlanOperation implements PlanOperation {
    private final String namespace;
    private final String evalId;
    private final String opType;

    /**
     * A constructor for DELETE operation.
     *
     * @param namespace a namespace of operation
     * @param evalId    a target evaluator id
     */
    public DeletePlanOperation(final String namespace, final String evalId) {
      this.namespace = namespace;
      this.evalId = evalId;
      this.opType = DEL_OP;
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
    public Optional<String> getEvalId() {
      return Optional.of(evalId);
    }

    @Override
    public String toString() {
      return "DeletePlanOperation{" +
          "namespace='" + namespace + '\'' +
          ", evalId='" + evalId + '\'' +
          ", opType='" + opType +'\'' +
          '}';
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final DeletePlanOperation that = (DeletePlanOperation) o;

      return namespace.equals(that.namespace) && evalId.equals(that.evalId);
    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + evalId.hashCode();
      result = 31 * result + opType.hashCode();
      return result;
    }

    @Override
    public Optional<TransferStep> getTransferStep() {
      return Optional.empty();
    }
  }

  public static final class MovePlanOperation implements PlanOperation {
    private final String namespace;
    private final TransferStep transferStep;
    private final String opType;


    /**
     * A constructor for MOVE operation.
     *
     * @param namespace    a namespace of operation
     * @param transferStep a TransferStep including src, dest, data info of MOVE operation
     */
    public MovePlanOperation(final String namespace, final TransferStep transferStep) {
      this.namespace = namespace;
      this.transferStep = transferStep;
      this.opType = MOVE_OP;
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
    public Optional<String> getEvalId() {
      return Optional.empty();
    }

    @Override
    public Optional<TransferStep> getTransferStep() {
      return Optional.of(transferStep);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final MovePlanOperation that = (MovePlanOperation) o;

      return namespace.equals(that.namespace) && transferStep.equals(that.transferStep);
    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + transferStep.hashCode();
      result = 31 * result + opType.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "MovePlanOperation{" +
          "namespace='" + namespace + '\'' +
          ", transferStep=" + transferStep +
          ", opType='" + opType +'\'' +
          '}';
    }
  }
}
