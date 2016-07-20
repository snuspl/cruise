/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.dolphin.async.optimizer;

import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;

/**
 * An implementation of EvaluatorParameters for workers.
 * A separate class for workers is needed in order to send {@link WorkerMetrics}
 */
public final class WorkerEvaluatorParametersImpl implements EvaluatorParameters {
  private final String id;
  private final DataInfo dataInfo;
  private final WorkerMetrics metrics;

  public WorkerEvaluatorParametersImpl(final String id,
                                       final DataInfo dataInfo,
                                       final WorkerMetrics metrics) {
    this.id = id;
    this.dataInfo = dataInfo;
    this.metrics = metrics;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public DataInfo getDataInfo() {
    return dataInfo;
  }

  public WorkerMetrics getMetrics() {
    return metrics;
  }
}
