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
import edu.snu.cay.services.et.exceptions.ExecutorNotExistException;
import edu.snu.cay.services.et.exceptions.PlanOpExecutionException;
import edu.snu.cay.services.et.metric.MetricManager;
import edu.snu.cay.services.et.metric.configuration.MetricServiceExecutorConf;
import edu.snu.cay.services.et.plan.impl.OpResult;
import org.apache.reef.tang.Configuration;

import java.util.Map;

/**
 * An operation for starting a task on an executor.
 */
public final class StartOp extends AbstractOp {
  private final String executorId;
  private final Configuration taskConf;
  private final MetricServiceExecutorConf metricConf;

  public StartOp(final String executorId,
                 final Configuration taskConf,
                 final MetricServiceExecutorConf metricConf) {
    super(OpType.START);
    this.executorId = executorId;
    this.taskConf = taskConf;
    this.metricConf = metricConf;
  }

  @Override
  public ListenableFuture<OpResult> execute(final ETMaster etMaster, final MetricManager metricManager,
                                            final Map<String, String> virtualIdToActualId)
      throws PlanOpExecutionException {
    final String actualId = virtualIdToActualId.containsKey(executorId) ?
        virtualIdToActualId.get(executorId) : executorId;

    final AllocatedExecutor executor;
    try {
      executor = etMaster.getExecutor(actualId);
    } catch (ExecutorNotExistException e) {
      throw new PlanOpExecutionException("Exception while executing " + toString(), e);
    }

    final ResultFuture<OpResult> resultFuture = new ResultFuture<>();

    // TODO #96: add a listener to sync
    metricManager.startMetricCollection(actualId, metricConf);
    executor.submitTask(taskConf)
        .addListener(task -> resultFuture.onCompleted(new OpResult.StartOpResult(StartOp.this, task)));

    return resultFuture;
  }

  @Override
  public String toString() {
    return "StartOp{" +
        "executorId='" + executorId + '\'' +
        ", taskConf=" + taskConf +
        '}';
  }
}
