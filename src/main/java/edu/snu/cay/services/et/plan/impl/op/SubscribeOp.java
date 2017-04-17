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

import edu.snu.cay.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.AllocatedTable;
import edu.snu.cay.services.et.exceptions.ExecutorNotExistException;
import edu.snu.cay.services.et.exceptions.PlanOpExecutionException;
import edu.snu.cay.services.et.exceptions.TableNotExistException;

import java.util.Collections;
import java.util.Map;

/**
 * An operation for making an executor subscribe ownership changes of a table.
 */
public final class SubscribeOp extends AbstractOp {
  private final String executorId;
  private final String tableId;

  public SubscribeOp(final String executorId,
                     final String tableId) {
    this.executorId = executorId;
    this.tableId = tableId;
  }

  @Override
  public ListenableFuture<?> execute(final ETMaster etMaster, final Map<String, String> virtualIdToActualId)
      throws PlanOpExecutionException {
    final AllocatedTable table;
    final AllocatedExecutor executor;
    try {
      table = etMaster.getTable(tableId);

      final String actualId = virtualIdToActualId.containsKey(executorId) ?
          virtualIdToActualId.get(executorId) : executorId;
      executor = etMaster.getExecutor(actualId);
    } catch (ExecutorNotExistException | TableNotExistException e) {
      throw new PlanOpExecutionException("Exception while executing " + toString(), e);
    }

    return table.subscribe(Collections.singletonList(executor));
  }

  @Override
  public String toString() {
    return "SubscribeOp{" +
        "executorId='" + executorId + '\'' +
        ", tableId='" + tableId + '\'' +
        '}';
  }
}
