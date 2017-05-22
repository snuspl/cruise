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
import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.exceptions.PlanOpExecutionException;
import edu.snu.cay.services.et.metric.MetricManager;
import edu.snu.cay.services.et.plan.impl.OpResult;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An operation for allocating a new executor.
 */
public final class AllocateOp extends AbstractOp {
  private static final Logger LOG = Logger.getLogger(AllocateOp.class.getName());

  private final String virtualId;
  private final ExecutorConfiguration executorConf;

  public AllocateOp(final String virtualId, final ExecutorConfiguration executorConf) {
    super(OpType.ALLOCATE);
    this.virtualId = virtualId;
    this.executorConf = executorConf;
  }

  @Override
  public ListenableFuture<OpResult> execute(final ETMaster etMaster, final MetricManager metricManager,
                                            final Map<String, String> virtualIdToActualId)
      throws PlanOpExecutionException {

    final ResultFuture<OpResult> opResultFuture = new ResultFuture<>();
    final ListenableFuture<List<AllocatedExecutor>> executorFuture = etMaster.addExecutors(1, executorConf);
    executorFuture.addListener(allocatedExecutorList -> {
      final AllocatedExecutor allocatedExecutor = allocatedExecutorList.get(0);

      virtualIdToActualId.put(virtualId, allocatedExecutor.getId());
      LOG.log(Level.INFO, "virtualId: {0}, actualId: {1}", new Object[]{virtualId, allocatedExecutor.getId()});

      opResultFuture.onCompleted(new OpResult.AllocateOpResult(AllocateOp.this, allocatedExecutor));
    });

    return opResultFuture;
  }

  @Override
  public String toString() {
    return "AllocateOp{" +
        "virtualId=" + virtualId +
        ", executorConf=" + executorConf +
        '}';
  }
}
