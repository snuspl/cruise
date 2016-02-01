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
package edu.snu.cay.dolphin.core.metric;

import edu.snu.cay.dolphin.core.metric.avro.MetricsMessage;
import edu.snu.cay.dolphin.core.metric.avro.SrcType;
import edu.snu.cay.dolphin.core.optimizer.OptimizationOrchestrator;
import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.utils.SingleMessageExtractor;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Driver-side message handler.
 * Receives metric messages and hands over them to {@link OptimizationOrchestrator}.
 */
@DriverSide
public final class DriverSideMetricsMsgHandler implements EventHandler<Message<MetricsMessage>> {
  private static final Logger LOG = Logger.getLogger(DriverSideMetricsMsgHandler.class.getName());

  private final OptimizationOrchestrator optimizationOrchestrator;
  private final MetricCodec metricCodec;

  @Inject
  private DriverSideMetricsMsgHandler(final OptimizationOrchestrator optimizationOrchestrator,
                                      final MetricCodec metricCodec) {
    this.optimizationOrchestrator = optimizationOrchestrator;
    this.metricCodec = metricCodec;
  }

  @Override
  public void onNext(final Message<MetricsMessage> msg) {
    LOG.entering(DriverSideMetricsMsgHandler.class.getSimpleName(), "onNext");
    final MetricsMessage metricsMessage = SingleMessageExtractor.extract(msg);

    final SrcType srcType = metricsMessage.getSrcType();
    if (SrcType.Compute.equals(srcType)) {

      optimizationOrchestrator.receiveComputeMetrics(msg.getSrcId().toString(),
          metricsMessage.getIterationInfo().getCommGroupName().toString(),
          metricsMessage.getIterationInfo().getIteration(),
          metricCodec.decode(metricsMessage.getMetrics().array()),
          getDataInfoFromAvro(metricsMessage.getComputeMsg().getDataInfos()));

    } else if (SrcType.Controller.equals(srcType)) {

      optimizationOrchestrator.receiveControllerMetrics(msg.getSrcId().toString(),
          metricsMessage.getIterationInfo().getCommGroupName().toString(),
          metricsMessage.getIterationInfo().getIteration(),
          metricCodec.decode(metricsMessage.getMetrics().array()));

    } else {
      throw new RuntimeException("Unknown SrcType " + srcType);
    }
    LOG.exiting(DriverSideMetricsMsgHandler.class.getSimpleName(), "onNext");
  }

  private List<DataInfo> getDataInfoFromAvro(
      final List<edu.snu.cay.dolphin.core.metric.avro.DataInfo> avroDataInfos) {
    final List<DataInfo> dataInfos = new ArrayList<>(avroDataInfos.size());
    for (final edu.snu.cay.dolphin.core.metric.avro.DataInfo avroDataInfo : avroDataInfos) {
      dataInfos.add(new DataInfoImpl(avroDataInfo.getDataType().toString(), avroDataInfo.getNumUnits()));
    }
    return dataInfos;
  }
}
