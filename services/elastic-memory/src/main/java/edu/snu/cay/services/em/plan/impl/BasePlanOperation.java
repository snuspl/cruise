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
 * A base implementation of PlanOperation.
 */
public class BasePlanOperation implements PlanOperation {
  private final String opType;
  private final String namespace;
  private final Optional<String> evalId;
  private final Optional<TransferStep> transferStep;

  public BasePlanOperation(final String opType, final String namespace, final String evalId) {
    this.opType = opType;
    this.namespace = namespace;
    this.evalId = Optional.of(evalId);
    this.transferStep = Optional.empty();
  }

  public BasePlanOperation(final String opType, final String namespace, final TransferStep transferStep) {
    this.opType = opType;
    this.namespace = namespace;
    this.evalId = Optional.empty();
    this.transferStep = Optional.of(transferStep);
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
    return evalId;
  }

  @Override
  public Optional<TransferStep> getTransferStep() {
    return transferStep;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final BasePlanOperation that = (BasePlanOperation) o;

    return opType.equals(that.opType) && namespace.equals(that.namespace)
        && evalId.equals(that.evalId) && transferStep.equals(that.transferStep);

  }

  @Override
  public int hashCode() {
    int result = opType.hashCode();
    result = 31 * result + namespace.hashCode();
    result = 31 * result + evalId.hashCode();
    result = 31 * result + transferStep.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "PlanOperation{" +
        "opType='" + opType + '\'' +
        ", namespace='" + namespace + '\'' +
        ", evalId=" + evalId +
        ", transferStep=" + transferStep +
        '}';
  }
}
