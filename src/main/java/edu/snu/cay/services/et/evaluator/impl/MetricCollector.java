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
package edu.snu.cay.services.et.evaluator.impl;

import edu.snu.cay.services.et.configuration.parameters.metric.CustomMetricCodec;
import edu.snu.cay.services.et.configuration.parameters.metric.MetricFlushPeriodMs;
import edu.snu.cay.services.et.evaluator.api.MessageSender;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Collects Metrics from ET and the customized Metrics running on ET.
 */
@EvaluatorSide
public final class MetricCollector<M> {
  private static final Logger LOG = Logger.getLogger(MetricCollector.class.getName());
  
  private final Tables tables;

  private final MessageSender msgSender;

  private final List<M> customMetrics;
  private final Codec<M> customMetricCodec;
  private final ScheduledExecutorService executor;
  private final long metricSendingPeriodMs;

  @Inject
  private MetricCollector(@Parameter(MetricFlushPeriodMs.class) final long metricSendingPeriodMs,
                          @Parameter(CustomMetricCodec.class) final Codec<M> customMetricCodec,
                          final Tables tables,
                          final MessageSender msgSender) {
    this.tables = tables;
    this.msgSender = msgSender;
    this.customMetrics = new LinkedList<>();
    this.customMetricCodec = customMetricCodec;
    this.metricSendingPeriodMs = metricSendingPeriodMs;
    this.executor =  Executors.newSingleThreadScheduledExecutor();
  }

  /**
   * Starts a thread that periodically sends collected metrics.
   */
  void start() {
    if (metricSendingPeriodMs > 0) {
      executor.scheduleWithFixedDelay(this::flush, metricSendingPeriodMs, metricSendingPeriodMs, TimeUnit.MILLISECONDS);
    } else if (metricSendingPeriodMs == 0) {
      LOG.log(Level.INFO, "MetricCollector does not send metrics periodically because metricSendingPeriodMs = 0. " +
          "Users instead can send manually via MetricCollector.send()");
    } else {
      throw new IllegalArgumentException("MetricFlushPeriodMs must be positive, but given " + metricSendingPeriodMs);
    }
    LOG.log(Level.INFO, "Metric collection has been started! Period: {0} ms", metricSendingPeriodMs);
  }

  /**
   * Stops the running thread for sending the metrics.
   * It flushes out all unsent metrics.
   */
  void stop() {
    flush();
    executor.shutdownNow();
    LOG.log(Level.INFO, "Metric collection has finished");
  }

  /**
   * Flushes the metrics collected so far. By default this method is called periodically
   * every {@link MetricFlushPeriodMs}.
   */
  public void flush() {
    final Map<String, Integer> tableIdToNumBlocks = tables.getTableToNumBlocks();

    LOG.log(Level.INFO, "Sending a metric containing {0} custom metrics: (tableToNumBlocks: {1}, customMetrics: {2})",
        new Object[] {customMetrics.size(), tableIdToNumBlocks, customMetrics});

    try {
      msgSender.sendMetricMsg(tableIdToNumBlocks, encodeCustomMetrics());
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Exception occurred", e);
    }
    customMetrics.clear();
  }

  /**
   * Adds custom metrics to be included in the message that will be sent next time.
   */
  public void addCustomMetric(final M metric) {
    customMetrics.add(metric);
  }

  private List<ByteBuffer> encodeCustomMetrics() {
    final List<ByteBuffer> encodedMetrics = new ArrayList<>(customMetrics.size());
    customMetrics.forEach(m -> encodedMetrics.add(ByteBuffer.wrap(customMetricCodec.encode(m))));

    return encodedMetrics;
  }
}
