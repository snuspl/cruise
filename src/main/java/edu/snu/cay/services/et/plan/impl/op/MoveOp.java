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
import edu.snu.cay.services.et.exceptions.NotAssociatedException;
import edu.snu.cay.services.et.exceptions.PlanOpExecutionException;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import edu.snu.cay.services.et.plan.impl.OpResult;

import java.util.Map;

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
    super(OpType.MOVE);
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
  public ListenableFuture<OpResult> execute(final ETMaster etMaster, final Map<String, String> virtualIdToActualId)
      throws PlanOpExecutionException {
    final AllocatedTable table;
    try {
      table = etMaster.getTable(tableId);
    } catch (TableNotExistException e) {
      throw new PlanOpExecutionException("Exception while executing " + toString(), e);
    }

    final String actualSrcId = virtualIdToActualId.containsKey(srcExecutorId) ?
        virtualIdToActualId.get(srcExecutorId) : srcExecutorId;
    final String actualDstId = virtualIdToActualId.containsKey(dstExecutorId) ?
        virtualIdToActualId.get(dstExecutorId) : dstExecutorId;

    final ResultFuture<OpResult> resultFuture = new ResultFuture<>();

    try {
      table.moveBlocks(actualSrcId, actualDstId, numBlocks)
          .addListener(migrationResult ->
              resultFuture.onCompleted(new OpResult.MoveOpResult(MoveOp.this, migrationResult)));
    } catch (NotAssociatedException e) {
      throw new PlanOpExecutionException(e);
    }

    return resultFuture;
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
