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
package edu.snu.cay.dolphin.async.dashboard;

import edu.snu.cay.dolphin.async.metric.avro.ServerMetrics;
import edu.snu.cay.dolphin.async.metric.avro.WorkerMetrics;
import edu.snu.cay.dolphin.async.dashboard.parameters.DashboardEnabled;
import edu.snu.cay.dolphin.async.dashboard.parameters.DashboardHostAddress;
import edu.snu.cay.dolphin.async.dashboard.parameters.DashboardPort;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class to connect to Dashboard and send metrics.
 */
public final class DashboardConnector {
  private static final Logger LOG = Logger.getLogger(DashboardConnector.class.getName());

  /**
   * Size of the queue for saving unsent metrics to the Dashboard server.
   */
  private static final int METRIC_QUEUE_SIZE = 1024;

  /**
   * Thread for sending metrics to dashboard server.
   */
  private final ExecutorService metricsSenderExecutor = Executors.newSingleThreadScheduledExecutor();

  /**
   * Metrics queue which saves unsent requests.
   */
  private final ArrayBlockingQueue<String> metricQueue = new ArrayBlockingQueue<>(METRIC_QUEUE_SIZE);


  private final DashboardSetupStatus dashboardSetupStatus;

  /**
   * Constructor of DashboardConnector.
   * @param hostAddress Host address of the dashboard server. The address is set lazily at the constructor
   *                    if the client has configured a feasible port number.
   * @param port        Port number of dolphin dashboard server.
   */
  @Inject
  private DashboardConnector(@Parameter(DashboardEnabled.class) final boolean dashboardEnabled,
                             @Parameter(DashboardHostAddress.class) final String hostAddress,
                             @Parameter(DashboardPort.class) final int port) {
    this.dashboardSetupStatus = dashboardEnabled ? initDashboard(hostAddress, port) : DashboardSetupStatus.getFailed();
  }

  private static final class DashboardSetupStatus {
    /**
     * {@code true} if the Dashboard server is in use.
     */
    private final boolean dashboardEnabled;

    /**
     * URL of Dolphin dashboard server. Empty if not using dashboard.
     */
    private final String dashboardURL;

    /**
     * Reusable HTTP client managed with PoolingHttpClientConnectionManager.
     */
    private final CloseableHttpAsyncClient reusableHttpClient;

    /**
     * A static object indicating failed status.
     */
    private static final DashboardSetupStatus FAILED = new DashboardSetupStatus(false, null, null);

    private DashboardSetupStatus(final boolean dashboardEnabled,
                                 final String dashboardURL,
                                 final CloseableHttpAsyncClient reusableHttpClient) {
      this.dashboardEnabled = dashboardEnabled;
      this.dashboardURL = dashboardURL;
      this.reusableHttpClient = reusableHttpClient;
    }

    static DashboardSetupStatus getFailed() {
      return FAILED;
    }

    static DashboardSetupStatus getSuccessful(final String dashboardURL,
                                              final CloseableHttpAsyncClient reusableHttpClient) {
      return new DashboardSetupStatus(true, dashboardURL, reusableHttpClient);
    }
  }

  private DashboardSetupStatus initDashboard(final String hostAddress, final int port) {
    final String dashboardURL = String.format("http://%s:%d/", hostAddress, port);
    try {
      // Create a pool of http client connection, which allow up to Integer.MAX_VALUE connections.
      final PoolingNHttpClientConnectionManager connectionManager
          = new PoolingNHttpClientConnectionManager(new DefaultConnectingIOReactor());
      connectionManager.setMaxTotal(Integer.MAX_VALUE);
      final CloseableHttpAsyncClient reusableHttpClient =
          HttpAsyncClients.custom().setConnectionManager(connectionManager).build();
      reusableHttpClient.start();

      // run another thread to send metrics.
      runMetricsSenderThread();

      return DashboardSetupStatus.getSuccessful(dashboardURL, reusableHttpClient);
    } catch (IOReactorException e) {
      LOG.log(Level.WARNING, "Dashboard: Fail on initializing connection to the dashboard server.", e);
      return DashboardSetupStatus.getFailed();
    }
  }

  /**
   * Send worker metrics to dashboard.
   * @param workerId worker id
   * @param metrics worker metrics
   */
  public void sendWorkerMetric(final String workerId, final WorkerMetrics metrics) {
    sendMetric(workerId, metrics.toString());
  }

  /**
   * Send server metrics to dashboard.
   * @param serverId worker id
   * @param metrics server metrics
   */
  public void sendServerMetric(final String serverId, final ServerMetrics metrics) {
    sendMetric(serverId, metrics.toString());
  }

  private void sendMetric(final String evalId, final String metrics) {
    // Regardless of metrics' validity, we send metrics to the dashboard for monitoring purpose.
    if (dashboardSetupStatus.dashboardEnabled) {
      try {
        metricQueue.put(String.format("id=%s&metrics=%s&time=%d",
            evalId, metrics, System.currentTimeMillis()));
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "Dashboard: Interrupted while taking metrics to send from the queue.", e);
      }
    }
  }

  /**
   * Runs a thread watching the metrics queue to send the metrics from the metricQueue via http request.
   */
  private void runMetricsSenderThread() {
    metricsSenderExecutor.execute(new Runnable() {
      @Override
      public void run() {
        while (true) {
          try {
            final String request = metricQueue.take();
            sendMetricsToDashboard(request);
          } catch (InterruptedException e) {
            LOG.log(Level.WARNING, "Dashboard: Interrupted while sending metrics to the dashboard server.", e);
          }
        }
      }

      /**
       * Send metrics to Dashboard server.
       *
       * @param request The POST request content which is to be sent to the dashboard server.
       */
      private void sendMetricsToDashboard(final String request) {
        try {
          final HttpPost httpPost = new HttpPost(dashboardSetupStatus.dashboardURL);
          httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded");
          httpPost.setEntity(new StringEntity(request));
          dashboardSetupStatus.reusableHttpClient.execute(httpPost, new FutureCallback<HttpResponse>() {
                @Override
                public void completed(final HttpResponse result) {
                  final int code = result.getStatusLine().getStatusCode();
                  if (code != HttpStatus.SC_OK) {
                    LOG.log(Level.WARNING, "Dashboard: Post request failed. Code-{0}", code);
                  }
                }

                @Override
                public void failed(final Exception ex) {
                  //TODO #772: deal with request failure.
                  LOG.log(Level.WARNING, "Dashboard: Post request failed.", ex);
                }

                @Override
                public void cancelled() {
                  //TODO #772: deal with request failure.
                  LOG.log(Level.WARNING, "Dashboard: Post request cancelled.");
                }
              }
          );
        } catch (IOException e) {
          //TODO #772: deal with request failure.
          LOG.log(Level.WARNING, "Dashboard: post request failed.", e);
        }
      }
    });
  }
}
