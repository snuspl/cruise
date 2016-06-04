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
package edu.snu.cay.dolphin.bsp.core.metric;

import edu.snu.cay.common.aggregation.avro.AggregationMessage;
import edu.snu.cay.common.metric.avro.Metrics;
import edu.snu.cay.dolphin.bsp.core.optimizer.OptimizationOrchestrator;
import edu.snu.cay.dolphin.bsp.core.metric.avro.MetricsMessage;
import edu.snu.cay.dolphin.bsp.core.metric.avro.SrcType;
import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Driver-side message handler.
 * Receives AggregationMessage and hands over them to {@link OptimizationOrchestrator}.
 */
@DriverSide
public final class DriverSideMetricsMsgHandler implements EventHandler<AggregationMessage> {
  private static final Logger LOG = Logger.getLogger(DriverSideMetricsMsgHandler.class.getName());

  private final OptimizationOrchestrator optimizationOrchestrator;
  private final MetricsMessageCodec metricsMessageCodec;

  @Inject
  private DriverSideMetricsMsgHandler(final OptimizationOrchestrator optimizationOrchestrator,
                                      final MetricsMessageCodec metricsMessageCodec) {
    this.optimizationOrchestrator = optimizationOrchestrator;
    this.metricsMessageCodec = metricsMessageCodec;
  }

  @Override
  public void onNext(final AggregationMessage msg) {
    LOG.entering(DriverSideMetricsMsgHandler.class.getSimpleName(), "onNext");
    final MetricsMessage metricsMessage = metricsMessageCodec.decode(msg.getData().array());

    final SrcType srcType = metricsMessage.getSrcType();

    if (SrcType.Compute.equals(srcType)) {

      optimizationOrchestrator.receiveComputeMetrics(msg.getSourceId().toString(),
          metricsMessage.getIterationInfo().getCommGroupName().toString(),
          metricsMessage.getIterationInfo().getIteration(),
          getMetricsFromAvro(metricsMessage.getMetrics()),
          getDataInfoFromAvro(metricsMessage.getComputeMsg().getDataInfo()));

    } else if (SrcType.Controller.equals(srcType)) {

      optimizationOrchestrator.receiveControllerMetrics(msg.getSourceId().toString(),
          metricsMessage.getIterationInfo().getCommGroupName().toString(),
          metricsMessage.getIterationInfo().getIteration(),
          getMetricsFromAvro(metricsMessage.getMetrics()));

    } else {
      throw new RuntimeException("Unknown SrcType " + srcType);
    }
    LOG.exiting(DriverSideMetricsMsgHandler.class.getSimpleName(), "onNext");
  }

  private DataInfo getDataInfoFromAvro(
      final edu.snu.cay.dolphin.bsp.core.metric.avro.DataInfo avroDataInfo) {
    return new DataInfoImpl(avroDataInfo.getNumUnits());
  }

  private Map<String, Double> getMetricsFromAvro(final Metrics avroMetrics) {
    final Map<String, Double> metrics = new HashMap<>(avroMetrics.getData().size());
    for (final Map.Entry<CharSequence, Double> metric : avroMetrics.getData().entrySet()) {
      metrics.put(metric.getKey().toString(), metric.getValue());
    }
    return metrics;
  }
}
