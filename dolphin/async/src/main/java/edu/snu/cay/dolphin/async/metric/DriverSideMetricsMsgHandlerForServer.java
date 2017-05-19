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
package edu.snu.cay.dolphin.async.metric;

import edu.snu.cay.common.centcomm.avro.CentCommMsg;
import edu.snu.cay.dolphin.async.metric.avro.ServerMetrics;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver-side message handler.
 * Receives CentCommMsg from Servers and hands over them to optimizer.
 */
@DriverSide
public final class DriverSideMetricsMsgHandlerForServer implements EventHandler<CentCommMsg> {
  private static final Logger LOG = Logger.getLogger(DriverSideMetricsMsgHandlerForServer.class.getName());

  private final ServerMetricsMsgCodec metricsMessageCodec;
  private final MetricManager metricManager;

  @Inject
  private DriverSideMetricsMsgHandlerForServer(final ServerMetricsMsgCodec metricsMessageCodec,
                                               final MetricManager metricManager) {
    this.metricsMessageCodec = metricsMessageCodec;
    this.metricManager = metricManager;
  }

  @Override
  public void onNext(final CentCommMsg msg) {
    LOG.entering(DriverSideMetricsMsgHandlerForServer.class.getSimpleName(), "onNext");
    final ServerMetrics metricsMessage = metricsMessageCodec.decode(msg.getData().array());

    final String serverId = msg.getSourceId().toString();

    LOG.log(Level.INFO, "ServerMetrics {0} {1}: {2}",
        new Object[]{System.currentTimeMillis(), serverId, metricsMessage});
    metricManager.storeServerMetrics(serverId, metricsMessage);

    LOG.exiting(DriverSideMetricsMsgHandlerForServer.class.getSimpleName(), "onNext");
  }
}
