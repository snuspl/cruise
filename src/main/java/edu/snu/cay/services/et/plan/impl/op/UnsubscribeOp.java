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
package edu.snu.cay.services.et.plan.impl.op;

import edu.snu.cay.services.et.common.util.concurrent.CompletedFuture;
import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.exceptions.PlanOpExecutionException;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An operation for stopping an executor from subscribing ownership changes of a table.
 */
public final class UnsubscribeOp extends AbstractOp {
  private static final Logger LOG = Logger.getLogger(UnsubscribeOp.class.getName());

  private final String executorId;
  private final String tableId;

  public UnsubscribeOp(final String executorId,
                       final String tableId) {
    this.executorId = executorId;
    this.tableId = tableId;
  }

  @Override
  public ListenableFuture<?> execute(final ETMaster etMaster, final Map<String, String> virtualIdToActualId)
      throws PlanOpExecutionException {
    LOG.log(Level.WARNING, "UnsubscribeOp has not been implemented yet (TODO #93).");
    // TODO #93: implement unsubscription routine
    return new CompletedFuture<>(null);
  }

  @Override
  public String toString() {
    return "UnsubscribeOp{" +
        "executorId='" + executorId + '\'' +
        ", tableId='" + tableId + '\'' +
        '}';
  }
}
