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
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.RunningTasklet;
import edu.snu.cay.services.et.exceptions.ExecutorNotExistException;
import edu.snu.cay.services.et.exceptions.PlanOpExecutionException;
import edu.snu.cay.services.et.metric.MetricManager;
import edu.snu.cay.services.et.plan.impl.OpResult;

import java.util.Collection;
import java.util.Map;

/**
 * An operation for stopping a running task on an executor.
 */
public final class StopOp extends AbstractOp {
  private final String executorId;

  public StopOp(final String executorId) {
    super(OpType.STOP);
    this.executorId = executorId;
  }

  @Override
  public ListenableFuture<OpResult> execute(final ETMaster etMaster, final MetricManager metricManager,
                                            final Map<String, String> virtualIdToActualId)
      throws PlanOpExecutionException {
    final AllocatedExecutor executor;
    try {
      executor = etMaster.getExecutor(executorId);
    } catch (ExecutorNotExistException e) {
      throw new PlanOpExecutionException(e);
    }

    final Collection<RunningTasklet> tasklets = executor.getRunningTasklets().values();
    if (tasklets.isEmpty()) {
      throw new PlanOpExecutionException("No running task on the executor " + executorId);
    }

    // TODO #00: fix. It assumes there's only one tasklet
    final RunningTasklet runningTasklet = tasklets.iterator().next();

    runningTasklet.stop();

    final ResultFuture<OpResult> opResultFuture = new ResultFuture<>();

    runningTasklet.getTaskResultFuture()
        .addListener(taskResult -> {
          // TODO #181: add a listener to sync
          // need to complete this op after metric service is stopped at executor
          metricManager.stopMetricCollection(executorId);
          opResultFuture.onCompleted(
              new OpResult.StopOpResult(StopOp.this));
        });

    return opResultFuture;
  }

  @Override
  public String toString() {
    return "StopOp{" +
        "executorId='" + executorId + '\'' +
        '}';
  }
}
