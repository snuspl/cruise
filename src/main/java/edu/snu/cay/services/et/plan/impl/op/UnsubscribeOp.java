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
import edu.snu.cay.services.et.common.util.concurrent.ResultFuture;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.AllocatedTable;
import edu.snu.cay.services.et.exceptions.PlanOpExecutionException;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import edu.snu.cay.services.et.metric.MetricManager;
import edu.snu.cay.services.et.plan.impl.OpResult;

import java.util.Map;

/**
 * An operation for stopping an executor from subscribing ownership changes of a table.
 */
public final class UnsubscribeOp extends AbstractOp {
  private final String executorId;
  private final String tableId;

  public UnsubscribeOp(final String executorId,
                       final String tableId) {
    super(OpType.UNSUBSCRIBE);
    this.executorId = executorId;
    this.tableId = tableId;
  }

  @Override
  public ListenableFuture<OpResult> execute(final ETMaster etMaster, final MetricManager metricManager,
                                            final Map<String, String> virtualIdToActualId)
      throws PlanOpExecutionException {
    final AllocatedTable table;
    try {
      table = etMaster.getTable(tableId);
    } catch (TableNotExistException e) {
      throw new PlanOpExecutionException("Exception while executing " + toString(), e);
    }

    final ResultFuture<OpResult> resultFuture = new ResultFuture<>();

    table.unsubscribe(executorId)
        .addListener(o -> resultFuture.onCompleted(new OpResult.UnsubscribeOpResult(UnsubscribeOp.this)));

    return resultFuture;
  }

  @Override
  public String toString() {
    return "UnsubscribeOp{" +
        "executorId='" + executorId + '\'' +
        ", tableId='" + tableId + '\'' +
        '}';
  }
}
