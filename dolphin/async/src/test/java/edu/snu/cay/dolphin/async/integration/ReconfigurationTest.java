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
package edu.snu.cay.dolphin.async.integration;

import edu.snu.cay.dolphin.async.examples.addinteger.AddIntegerREEF;
import edu.snu.cay.dolphin.async.examples.addvector.AddVectorREEF;
import edu.snu.cay.dolphin.async.plan.AsyncDolphinPlanExecutor;
import edu.snu.cay.dolphin.async.optimizer.OptimizationOrchestrator;
import edu.snu.cay.utils.TestLoggingConfig;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Reconfiguration test that uses {@link AddIntegerREEF} example app.
 * It runs the app with {@link TestingOrchestrator} that runs several optimization plans
 * to confirm that dolphin reconfigures the system correctly and reliably.
 */
public final class ReconfigurationTest {

  @Test
  public void testReconfigurationWithSampleOptimizers() {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(OptimizationOrchestrator.class, TestingOrchestrator.class)
        .build();

    System.setProperty("java.util.logging.config.class", TestLoggingConfig.class.getName());

    final List<String> args = getArguments();

    assertEquals("The job has been failed", LauncherStatus.COMPLETED,
        AddIntegerREEF.runAddInteger((String[]) args.toArray(), conf));

    args.add("-vector_size");
    args.add(Integer.toString(5));

    assertEquals("The job has been failed", LauncherStatus.COMPLETED,
        AddVectorREEF.runAddVector((String[]) args.toArray(), conf));
  }

  private List<String> getArguments() {
    final int numWorkers = 3;
    final int numServers = 2;
    final int numTotalEvals = numWorkers + numServers; // do not use more resources

    return Arrays.asList(
        "-split", Integer.toString(numWorkers),
        "-num_workers", Integer.toString(numWorkers),
        "-num_servers", Integer.toString(numServers),
        "-max_num_eval_local", Integer.toString(numTotalEvals),
        "-max_num_epochs", Integer.toString(10),
        "-mini_batch_size", Integer.toString(10),
        "-delta", Integer.toString(4),
        "-num_keys", Integer.toString(100),
        "-input", ClassLoader.getSystemResource("data").getPath() + "/empty_file",
        "-dynamic", Boolean.toString(true),
        "-plan_executor", AsyncDolphinPlanExecutor.class.getName(),
        "-compute_time_ms", Integer.toString(30),
        "-num_training_data", Integer.toString(100),
        "-optimization_interval_ms", Integer.toString(3000),
        "-delay_after_optimization_ms", Integer.toString(10000),
        "-worker_log_period_ms", Integer.toString(0),
        "-server_log_period_ms", Integer.toString(0),
        "-server_metrics_window_ms", Integer.toString(1000),
        "-timeout", Integer.toString(300000));
  }
}
