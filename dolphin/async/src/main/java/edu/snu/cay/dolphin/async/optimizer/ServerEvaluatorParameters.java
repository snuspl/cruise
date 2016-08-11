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

import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.ps.metric.avro.ServerMetrics;

/**
 * An implementation of EvaluatorParameters for servers.
 * A separate class for servers is needed in order to send {@link ServerMetrics}
 */
public final class ServerEvaluatorParameters implements EvaluatorParameters<ServerMetrics> {
  private final String id;
  private final DataInfo dataInfo;
  private final ServerMetrics metrics;

  /**
   * Constructs a server evaluator's current status.
   * @param id the evaluator ID
   * @param dataInfo {@link DataInfo} representing the number of blocks allocated
   * @param metrics {@link ServerMetrics} representing the server's iteration execution metrics
   */
  public ServerEvaluatorParameters(final String id,
                                   final DataInfo dataInfo,
                                   final ServerMetrics metrics) {
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

  @Override
  public ServerMetrics getMetrics() {
    return metrics;
  }

  @Override
  public String toString() {
    return "ServerEvaluatorParameters{" +
        "id='" + id + '\'' +
        ", dataInfo=" + dataInfo +
        ", metrics=" + metrics +
        '}';
  }
}
