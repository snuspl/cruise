/*
 * Copyright (C) 2016 Seoul National University
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
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.ps.metric.avro.ServerMetrics;

import javax.inject.Inject;
import java.util.*;

/**
 * A temporary storage for holding worker and server metrics related to optimization.
 */
public final class MetricsHub {

  /**
   * Worker-side metrics, each in the form of a {@link EvaluatorParameters} object.
   */
  private final List<EvaluatorParameters> workerEvalParams;

  /**
   * Server-side metrics, each in the form of a {@link EvaluatorParameters} object.
   */
  private final List<EvaluatorParameters> serverEvalParams;

  @Inject
  private MetricsHub() {
    this.workerEvalParams = Collections.synchronizedList(new LinkedList<>());
    this.serverEvalParams = Collections.synchronizedList(new LinkedList<>());
  }

  /**
   * Store a {@link EvaluatorParameters} object for a parameter set of a certain worker.
   * This method does not override existing metrics with the same {@code workerId}.
   * Instead, a new {@link EvaluatorParameters} object is allocated for each call.
   */
  public void storeWorkerMetrics(final String workerId, final WorkerMetrics metrics) {
    final DataInfo dataInfo = new DataInfoImpl(metrics.getNumDataBlocks());
    final EvaluatorParameters evaluatorParameters = new WorkerEvaluatorParametersImpl(workerId, dataInfo, metrics);
    workerEvalParams.add(evaluatorParameters);
  }

  /**
   * Store a {@link EvaluatorParameters} object for a parameter set of a certain server.
   * This method does not override existing metrics with the same {@code serverId}.
   * Instead, a new {@link EvaluatorParameters} object is allocated for each call.
   */
  public void storeServerMetrics(final String serverId, final ServerMetrics metrics) {
    final DataInfo dataInfo = new DataInfoImpl(metrics.getNumPartitionBlocks());
    final EvaluatorParameters evaluatorParameters = new ServerEvaluatorParametersImpl(serverId, dataInfo, metrics);
    serverEvalParams.add(evaluatorParameters);
  }

  /**
   * Empty out the current set of worker metrics and return them.
   */
  public List<EvaluatorParameters> drainWorkerMetrics() {
    synchronized (workerEvalParams) {
      final List<EvaluatorParameters> currWorkerMetrics = new ArrayList<>(workerEvalParams);
      workerEvalParams.clear();
      return currWorkerMetrics;
    }
  }

  /**
   * Empty out the current set of server metrics and return them.
   */
  public List<EvaluatorParameters> drainServerMetrics() {
    synchronized (serverEvalParams) {
      final List<EvaluatorParameters> currServerMetrics = new ArrayList<>(serverEvalParams);
      serverEvalParams.clear();
      return currServerMetrics;
    }
  }
}
