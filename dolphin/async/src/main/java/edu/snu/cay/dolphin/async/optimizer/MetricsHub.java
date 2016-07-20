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
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.em.optimizer.impl.EvaluatorParametersImpl;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
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
  public void storeWorkerMetrics(final String workerId, final int numDataBlocks,
                                 final Map<String, Double> metrics) {
    final DataInfo dataInfo = new DataInfoImpl(numDataBlocks);
    final EvaluatorParameters evaluatorParameters = new EvaluatorParametersImpl(workerId, dataInfo, metrics);
    workerEvalParams.add(evaluatorParameters);
    try {
      final URL obj = new URL("http://127.0.0.1:5000/metrics/");
      final HttpURLConnection con = (HttpURLConnection) obj.openConnection();
      con.setRequestMethod("POST");
      con.setDoOutput(true);
      con.setDoInput(true);
      con.connect();
      // request
      final OutputStream os = con.getOutputStream();
      final String param = "serverid=-1&workerid=" + workerId;
      System.out.println("request with " + param);
      os.write((param).getBytes());
      os.flush();
      os.close();
      // response
      final BufferedReader in = new BufferedReader(
          new InputStreamReader(con.getInputStream())
      );
      String inputLine;
      final StringBuffer response = new StringBuffer();
      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
      in.close();
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  /**
   * Store a {@link EvaluatorParameters} object for a parameter set of a certain server.
   * This method does not override existing metrics with the same {@code serverId}.
   * Instead, a new {@link EvaluatorParameters} object is allocated for each call.
   */
  public void storeServerMetrics(final String serverId, final int numPartitionBlocks,
                                 final Map<String, Double> metrics) {
    final DataInfo dataInfo = new DataInfoImpl(numPartitionBlocks);
    final EvaluatorParameters evaluatorParameters = new EvaluatorParametersImpl(serverId, dataInfo, metrics);
    serverEvalParams.add(evaluatorParameters);
    try {
      final URL obj = new URL("http://127.0.0.1:5000/metrics/");
      final HttpURLConnection con = (HttpURLConnection) obj.openConnection();
      con.setRequestMethod("POST");
      con.setDoOutput(true);
      con.setDoInput(true);
      con.connect();
      // request
      final OutputStream os = con.getOutputStream();
      final String param = "workerid=-1&serverid=" + serverId;
      System.out.println("request with " + param);
      os.write((param).getBytes());
      os.flush();
      os.close();
      // response
      final BufferedReader in = new BufferedReader(
          new InputStreamReader(con.getInputStream())
      );
      String inputLine;
      final StringBuffer response = new StringBuffer();
      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
      in.close();
    } catch (Exception e) {
      System.out.println(e);
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
