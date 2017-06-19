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
package edu.snu.cay.dolphin.async.jobserver;


import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.configuration.ResourceConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver code for Dolphin on ET.
 * Upon start, it initializes executors and tables for running a dolphin job.
 */
@Unit
public final class JobServerDriver {

  private static final Logger LOG = Logger.getLogger(JobServerDriver.class.getName());

  private final ETMaster etMaster;
  private final JobMessageObserver jobMessageObserver;
  private final HttpServerInfo httpServerInfo;
  private final String jobId;
  private final Map<String, Pair<List<AllocatedExecutor>, List<AllocatedExecutor>>> jobToExecutorsMap;

  private static final ExecutorConfiguration EXECUTOR_CONF = ExecutorConfiguration.newBuilder()
      .setResourceConf(
          ResourceConfiguration.newBuilder()
              .setNumCores(1)
              .setMemSizeInMB(128)
              .build())
      .build();

  @Inject
  private JobServerDriver(final ETMaster etMaster,
                          @Parameter(JobIdentifier.class) final String jobId,
                          final JobMessageObserver jobMessageObserver,
                          final HttpServerInfo httpServerInfo)
      throws IOException, InjectionException {
    this.etMaster = etMaster;
    this.jobMessageObserver = jobMessageObserver;
    this.httpServerInfo = httpServerInfo;
    this.jobId = jobId;
    jobToExecutorsMap = new HashMap<>();
  }

  /**
   * Submits a job by a specific {@link JobConfiguration}.
   * All commands are received by {@link JobServerHttpHandler}.
   */
  public void submitJob(final JobConfiguration jobConf) {
    //TODO #1173: Implement runtime job submission.
    LOG.log(Level.INFO, "Job submission command is received : job id : {0}, " +
        "number of server : {1}, " + "number of worker : {2}",
        new Object[]{jobConf.getJobId(), jobConf.getNumServer(), jobConf.getNumWorker()});
  }

  /**
   * Terminates all jobs that were running and closes all executors used for each job.
   */
  public void finishServer() {
    //TODO #1173: Implement job server completion.
    jobToExecutorsMap.values().forEach(pair -> {
      pair.getLeft().forEach(AllocatedExecutor::close);
      pair.getRight().forEach(AllocatedExecutor::close);
    });
    LOG.log(Level.INFO, "Job server is finished");
  }

  /**
   * A driver start handler for requesting executors.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      try {

        showHttpInfoToClient(httpServerInfo.getLocalAddress(), httpServerInfo.getPort());
        final List<AllocatedExecutor> servers = etMaster.addExecutors(2, EXECUTOR_CONF).get();
        final List<AllocatedExecutor> workers = etMaster.addExecutors(2, EXECUTOR_CONF).get();

        Executors.newSingleThreadExecutor().submit(() -> jobToExecutorsMap.put(jobId, Pair.of(servers, workers)));
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Handler for FailedContext, which throws RuntimeException to shutdown the entire job.
   */
  final class FailedContextHandler implements EventHandler<FailedContext> {
    @Override
    public void onNext(final FailedContext failedContext) {
      // TODO #677: Handle failure from Evaluators properly
      throw new RuntimeException(failedContext.asError());
    }
  }

  /**
   * Handler for FailedEvaluator, which throws RuntimeException to shutdown the entire job.
   */
  final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      // TODO #677: Handle failure from Evaluators properly
      throw new RuntimeException(failedEvaluator.getEvaluatorException());
    }
  }

  /**
   * Handler for FailedTask, which throws RuntimeException to shutdown the entire job.
   */
  final class FailedTaskHandler implements EventHandler<FailedTask> {
    @Override
    public void onNext(final FailedTask failedTask) {
      // TODO #677: Handle failure from Evaluators properly
      throw new RuntimeException(failedTask.asError());
    }
  }

  private void showHttpInfoToClient(final String localAddress, final int portNumber) {
    final String httpMsg = String.format(
        "\nIP address : %s\n " +
        "Port : %d\n " +
        "Usage : http://%s:%d/dolphin/v1/conf={#jobConf}",
        localAddress, portNumber, localAddress, portNumber);
    jobMessageObserver.sendMessageToClient(httpMsg.getBytes());
  }
}
