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
package edu.snu.spl.cruise.services.et.plan.impl.op;

import edu.snu.spl.cruise.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.spl.cruise.services.et.common.util.concurrent.ResultFuture;
import edu.snu.spl.cruise.services.et.configuration.TaskletConfiguration;
import edu.snu.spl.cruise.services.et.driver.api.AllocatedExecutor;
import edu.snu.spl.cruise.services.et.driver.api.ETMaster;
import edu.snu.spl.cruise.services.et.exceptions.ExecutorNotExistException;
import edu.snu.spl.cruise.services.et.exceptions.PlanOpExecutionException;
import edu.snu.spl.cruise.services.et.metric.MetricManager;
import edu.snu.spl.cruise.services.et.metric.configuration.MetricServiceExecutorConf;
import edu.snu.spl.cruise.services.et.plan.impl.OpResult;

import java.util.Map;

/**
 * An operation for starting a task on an executor.
 */
public final class StartOp extends AbstractOp {
  private final String executorId;
  private final TaskletConfiguration taskConf;
  private final MetricServiceExecutorConf metricConf;

  public StartOp(final String executorId,
                 final TaskletConfiguration taskConf,
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

    // TODO #181: add a listener to sync
    // need to submit task after metric service is started at executor
    metricManager.startMetricCollection(actualId, metricConf);

    executor.submitTasklet(taskConf)
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
