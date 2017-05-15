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
package edu.snu.cay.services.et.metric;

import edu.snu.cay.services.et.evaluator.api.MessageSender;
import edu.snu.cay.services.et.evaluator.impl.RemoteAccessOpStat;
import edu.snu.cay.services.et.evaluator.impl.Tables;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.serialization.Codec;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Collects Metrics from ET and the customized Metrics running on ET.
 * Users should add metrics for a specific type only during start-stop interval for that type of metrics.
 */
@EvaluatorSide
@ThreadSafe
public final class MetricCollector {
  private static final Logger LOG = Logger.getLogger(MetricCollector.class.getName());

  private final Tables tables;

  private final MessageSender msgSender;

  private final RemoteAccessOpStat remoteAccessOpStat;

  private final List<Object> customMetrics;
  private final ScheduledExecutorService executor;

  private Codec customMetricCodec;

  @Inject
  private MetricCollector(final Tables tables,
                          final MessageSender msgSender,
                          final RemoteAccessOpStat remoteAccessOpStat) {
    this.tables = tables;
    this.msgSender = msgSender;
    this.remoteAccessOpStat = remoteAccessOpStat;
    this.customMetrics = Collections.synchronizedList(new LinkedList<>());
    this.executor = Executors.newSingleThreadScheduledExecutor();
  }

  /**
   * Starts a thread that periodically sends collected metrics.
   */
  public synchronized void start(final long metricSendingPeriodMs, final Codec metricCodec) {
    customMetrics.clear();
    customMetricCodec = metricCodec;

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
  public synchronized void stop() {
    executor.shutdownNow();
    flush();

    customMetrics.clear();
    customMetricCodec = null;
    LOG.log(Level.INFO, "Metric collection has finished");
  }

  /**
   * Flushes the metrics collected so far.
   */
  public void flush() {
    final Map<String, Integer> tableIdToNumBlocks = tables.getTableToNumBlocks();
    final Map<String, Long> bytesReceivedGetResp = remoteAccessOpStat.getBytesReceivedGetResp();
    final Map<String, Integer> countsSentGetReq = remoteAccessOpStat.getCountsSentGetReq();

    final List<Object> copiedMetrics;
    synchronized (customMetrics) {
      copiedMetrics = new ArrayList<>(customMetrics);
      customMetrics.clear();
    }

    LOG.log(Level.INFO, "Sending a metric containing {0} custom metrics: (tableToNumBlocks: {1}, customMetrics: {2})",
        new Object[] {copiedMetrics.size(), tableIdToNumBlocks, copiedMetrics});


    try {
      msgSender.sendMetricReportMsg(tableIdToNumBlocks, bytesReceivedGetResp, countsSentGetReq,
          encodeCustomMetrics(copiedMetrics));
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Exception occurred", e);
    }
  }

  /**
   * Adds custom metrics to be included in the message that will be sent next time.
   */
  public void addCustomMetric(final Object metric) {
    customMetrics.add(metric);
  }

  private List<ByteBuffer> encodeCustomMetrics(final List<Object> metrics) {
    final List<ByteBuffer> encodedMetrics = new ArrayList<>(metrics.size());
    metrics.forEach(m -> encodedMetrics.add(ByteBuffer.wrap(customMetricCodec.encode(m))));
    return encodedMetrics;
  }
}
