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
package edu.snu.cay.dolphin.async.plan.impl;

import edu.snu.cay.dolphin.async.plan.api.TransferStep;

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

  public static final class AddPlanOperation extends BasePlanOperation {

    /**
     * A constructor for ADD operation.
     *
     * @param namespace a namespace of operation
     * @param evalId    a target evaluator id
     */
    public AddPlanOperation(final String namespace, final String evalId) {
      super(ADD_OP, namespace, evalId);
    }
  }

  public static final class DeletePlanOperation extends BasePlanOperation {

    /**
     * A constructor for DELETE operation.
     *
     * @param namespace a namespace of operation
     * @param evalId    a target evaluator id
     */
    public DeletePlanOperation(final String namespace, final String evalId) {
      super(DEL_OP, namespace, evalId);
    }
  }

  public static final class MovePlanOperation extends BasePlanOperation {

    /**
     * A constructor for MOVE operation.
     *
     * @param namespace    a namespace of operation
     * @param transferStep a TransferStep including src, dest, data info of MOVE operation
     */
    public MovePlanOperation(final String namespace, final TransferStep transferStep) {
      super(MOVE_OP, namespace, transferStep);
    }
  }
}
