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
import edu.snu.cay.services.em.plan.impl.BasePlanOperation;
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

  static final class SyncPlanOperation extends BasePlanOperation {

    /**
     * A constructor for SYNC operation.
     *
     * @param evalId    a target evaluator id
     */
    SyncPlanOperation(final String evalId) {
      super(SYNC_OP, Constants.NAMESPACE_SERVER, evalId);
    }
  }

  static final class StartPlanOperation extends BasePlanOperation {

    /**
     * A constructor for START operation.
     *
     * @param evalId    a target evaluator id
     */
    StartPlanOperation(final String evalId) {
      super(START_OP, Constants.NAMESPACE_WORKER, evalId);
    }
  }

  static final class StopPlanOperation extends BasePlanOperation {

    /**
     * A constructor for STOP operation.
     *
     * @param evalId    a target evaluator id
     */
    StopPlanOperation(final String evalId) {
      super(STOP_OP, Constants.NAMESPACE_WORKER, evalId);
    }
  }
}
