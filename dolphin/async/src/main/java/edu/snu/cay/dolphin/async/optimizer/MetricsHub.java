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

import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.ps.metric.avro.ServerMetrics;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A temporary storage for holding worker and server metrics related to optimization.
 * Users can optionally run a dashboard server, which visualizes the received metrics. {@link DashboardPort}
 */
public final class MetricsHub {
  private static final Logger LOG = Logger.getLogger(MetricsHub.class.getName());
  /**
   * Worker-side metrics, each in the form of a {@link EvaluatorParameters} object.
   */
  private final List<EvaluatorParameters> workerEvalParams;

  /**
   * Server-side metrics, each in the form of a {@link EvaluatorParameters} object.
   */
  private final List<EvaluatorParameters> serverEvalParams;

  /**
   * URL of Dolphin dashboard server. Empty if not using dashboard.
   */
  private final String dashboardURL;

  /**
   * If the Dashboard server is in use.
   */
  private final boolean dashboardEnabled;

  @Inject
  private MetricsHub(@Parameter(Parameters.DashboardHostAddress.class) final String hostAddress,
                     @Parameter(Parameters.DashboardPort.class) final int port,
                     @Parameter(Parameters.DashboardEnabled.class) final boolean dashboardEnabled) {
    this.workerEvalParams = Collections.synchronizedList(new LinkedList<>());
    this.serverEvalParams = Collections.synchronizedList(new LinkedList<>());
    this.dashboardEnabled = dashboardEnabled;
    if (dashboardEnabled) {
      this.dashboardURL = "http://" + hostAddress + ":" + port + "/";
    } else {
      this.dashboardURL = "";
    }
    LOG.log(Level.INFO, "Dashboard: " + dashboardEnabled + " " + dashboardURL);
  }

  /**
   * Send metrics to Dashboard server.
   * @param id ID of the part which is sending the metrics.
   * @param metrics The metrics to send to the Dashboard server.
   */
  private void sendMetrics(final String id, final String metrics) {
    try {
      LOG.log(Level.INFO, "send metrics to " + dashboardURL);
      LOG.log(Level.INFO, "send " + metrics);
      // Build http connection with the Dashboard server, set configurations.
      // TODO #722: Create WebSocket instead of connecting every time to send metrics to Dashboard server
      final String dashboardUrlStr = this.dashboardURL;
      final URL dashboardUrl = new URL(dashboardUrlStr);
      final HttpURLConnection con = (HttpURLConnection) dashboardUrl.openConnection();
      con.setRequestMethod("POST");
      con.setDoOutput(true);
      con.setDoInput(true);
      con.connect();

      // Send metrics via outputStream to the Dashboard server.
      try (final OutputStream os = con.getOutputStream()) {
        final String param = "id=" + id + "&metrics=" + metrics + "&time=" + System.currentTimeMillis();
        os.write((param).getBytes());
        os.flush();
      }

      // Receive responses from the Dashboard Server.
      try (final BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()))) {
        String inputLine;
        final StringBuffer response = new StringBuffer();
        while ((inputLine = in.readLine()) != null) {
          response.append(inputLine);
        }
      }

    } catch (Exception e) {
      LOG.log(Level.WARNING, "Failed to send metrics to Dashboard server.", e);
    }
  }

  /**
   * Store a {@link EvaluatorParameters} object for a parameter set of a certain worker.
   * This method does not override existing metrics with the same {@code workerId}.
   * Instead, a new {@link EvaluatorParameters} object is allocated for each call.
   */
  public void storeWorkerMetrics(final String workerId, final WorkerMetrics metrics) {
    final DataInfo dataInfo = new DataInfoImpl(metrics.getNumDataBlocks());
    final EvaluatorParameters evaluatorParameters = new WorkerEvaluatorParameters(workerId, dataInfo, metrics);
    workerEvalParams.add(evaluatorParameters);
    if (this.dashboardEnabled) {
      sendMetrics(workerId, metrics.toString());
    }
  }

  /**
   * Store a {@link EvaluatorParameters} object for a parameter set of a certain server.
   * This method does not override existing metrics with the same {@code serverId}.
   * Instead, a new {@link EvaluatorParameters} object is allocated for each call.
   */
  public void storeServerMetrics(final String serverId, final ServerMetrics metrics) {
    final DataInfo dataInfo = new DataInfoImpl(metrics.getNumPartitionBlocks());
    final EvaluatorParameters evaluatorParameters = new ServerEvaluatorParameters(serverId, dataInfo, metrics);
    serverEvalParams.add(evaluatorParameters);
    if (this.dashboardEnabled) {
      sendMetrics(serverId, metrics.toString());
    }
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
