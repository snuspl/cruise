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
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.AllocatedTable;
import edu.snu.cay.services.et.exceptions.PlanOpExecutionException;
import edu.snu.cay.services.et.exceptions.TableNotExistException;

/**
 * An operation for moving table blocks between executors.
 */
public final class MoveOp extends AbstractOp {
  private final String srcExecutorId;
  private final String dstExecutorId;
  private final String tableId;
  private final int numBlocks;

  public MoveOp(final String srcExecutorId, final String dstExecutorId,
                final String tableId, final int numBlocks) {
    this.srcExecutorId = srcExecutorId;
    this.dstExecutorId = dstExecutorId;
    this.tableId = tableId;
    this.numBlocks = numBlocks;
  }

  public String getSrcExecutorId() {
    return srcExecutorId;
  }

  public String getDstExecutorId() {
    return dstExecutorId;
  }

  @Override
  public ListenableFuture<?> execute(final ETMaster etMaster) throws PlanOpExecutionException {
    final AllocatedTable table;
    try {
      table = etMaster.getTable(tableId);
    } catch (TableNotExistException e) {
      throw new PlanOpExecutionException("Exception while executing " + toString(), e);
    }

    return table.moveBlocks(srcExecutorId, dstExecutorId, numBlocks);
  }

  @Override
  public String toString() {
    return "MoveOp{" +
        "srcExecutorId='" + srcExecutorId + '\'' +
        ", dstExecutorId='" + dstExecutorId + '\'' +
        ", tableId='" + tableId + '\'' +
        ", numBlocks=" + numBlocks +
        '}';
  }
}
