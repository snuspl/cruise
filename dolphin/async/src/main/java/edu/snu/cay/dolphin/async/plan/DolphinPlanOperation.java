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

import edu.snu.cay.dolphin.async.optimizer.parameters.Constants;
import edu.snu.cay.services.em.plan.api.PlanOperation;
import edu.snu.cay.services.em.plan.api.TransferStep;
import org.apache.reef.util.Optional;

/**
 * A class representing Dolphin-specific plan operation.
 * Regarding to EM's ADD/DEL, Dolphin's worker tasks should be controlled
 * to prevent them from running task iterations with empty dataset.
 */
final class DolphinPlanOperation {
  static final String SYNC_OP = "SYNC"; // sync server's ownership state between driver and workers
  static final String START_OP = "START"; // start worker task
  static final String STOP_OP = "STOP"; // stop worker task

  /**
   * Should not be instantiated.
   */
  private DolphinPlanOperation() {

  }

  static final class SyncPlanOperation implements PlanOperation {
    private final String namespace;
    private final String evalId;
    private final String opType;

    /**
     * A constructor for SYNC operation.
     *
     * @param evalId    a target evaluator id
     */
    SyncPlanOperation(final String evalId) {
      this.namespace = Constants.NAMESPACE_SERVER;
      this.evalId = evalId;
      this.opType = SYNC_OP;
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

      final SyncPlanOperation that = (SyncPlanOperation) o;

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
      return "SyncPlanOperation{" +
          "namespace='" + namespace + '\'' +
          ", evalId='" + evalId + '\'' +
          ", opType='" + opType + '\'' +
          '}';
    }
  }

  static final class StartPlanOperation implements PlanOperation {
    private final String namespace;
    private final String evalId;
    private final String opType;

    /**
     * A constructor for START operation.
     *
     * @param evalId    a target evaluator id
     */
    StartPlanOperation(final String evalId) {
      this.namespace = Constants.NAMESPACE_WORKER;
      this.evalId = evalId;
      this.opType = START_OP;
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

      final StartPlanOperation that = (StartPlanOperation) o;

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
      return "StartPlanOperation{" +
          "namespace='" + namespace + '\'' +
          ", evalId='" + evalId + '\'' +
          ", opType='" + opType + '\'' +
          '}';
    }
  }

  static final class StopPlanOperation implements PlanOperation {
    private final String namespace;
    private final String evalId;
    private final String opType;

    /**
     * A constructor for STOP operation.
     *
     * @param evalId    a target evaluator id
     */
    StopPlanOperation(final String evalId) {
      this.namespace = Constants.NAMESPACE_WORKER;
      this.evalId = evalId;
      this.opType = STOP_OP;
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

      final StopPlanOperation that = (StopPlanOperation) o;

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
      return "StopPlanOperation{" +
          "namespace='" + namespace + '\'' +
          ", evalId='" + evalId + '\'' +
          ", opType='" + opType + '\'' +
          '}';
    }
  }
}
