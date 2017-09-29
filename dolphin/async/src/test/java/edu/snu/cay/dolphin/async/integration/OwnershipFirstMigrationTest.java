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
package edu.snu.cay.dolphin.async.integration;

import edu.snu.cay.dolphin.async.examples.addvector.AddVectorET;
import edu.snu.cay.dolphin.async.optimizer.impl.SampleOptimizers;
import edu.snu.cay.utils.test.IntegrationTest;
import org.apache.reef.client.LauncherStatus;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Integration test of OwnershipFirstMigration by using {@link AddVectorET} example app
 * that fails when server's data values at the final are different from the expectation.
 * It runs the app with several optimization plans that add and delete servers
 * to confirm that this migration protocol preserves data values during migration correctly.
 */
@Category(IntegrationTest.class)
public final class OwnershipFirstMigrationTest {

  @Test
  public void testOwnershipFirstMigrationByDeletingOneServer() {
    final int numWorkers = 3;
    final int numServers = 2;
    final int numTotalEvals = numWorkers + numServers;

    final List<String> defaultArgList = getDefaultArguments(numWorkers, numServers);

    final List<String> argListForDeletingOneServer = Arrays.asList(
        "-num_workers", Integer.toString(numWorkers),
        "-number_workers", Integer.toString(numWorkers),
        "-number_servers", Integer.toString(numServers),
        "-max_num_eval_local", Integer.toString(numTotalEvals),
        "-optimizer", SampleOptimizers.DeleteOneServerOptimizer.class.getName()
    );

    final List<String> argList = new ArrayList<>(defaultArgList.size() + argListForDeletingOneServer.size());
    argList.addAll(defaultArgList);
    argList.addAll(argListForDeletingOneServer);

    final String[] args = argList.toArray(new String[defaultArgList.size() + argListForDeletingOneServer.size()]);
    assertEquals("The job has been failed", LauncherStatus.COMPLETED, AddVectorET.runAddVector(args));
  }

  @Test
  public void testOwnershipFirstMigrationByAddingOneServer() {
    final int numWorkers = 3;
    final int numServers = 2;
    final int numTotalEvals = numWorkers + numServers + SampleOptimizers.MAX_CALLS_TO_MAKE;

    final List<String> defaultArgList = getDefaultArguments(numWorkers, numServers);

    final List<String> argListForAddingOneServer = Arrays.asList(
        "-num_workers", Integer.toString(numWorkers),
        "-number_workers", Integer.toString(numWorkers),
        "-number_servers", Integer.toString(numServers),
        "-max_num_eval_local", Integer.toString(numTotalEvals),
        "-optimizer", SampleOptimizers.AddOneServerOptimizer.class.getName()
    );

    final List<String> argList = new ArrayList<>(defaultArgList.size() + argListForAddingOneServer.size());
    argList.addAll(defaultArgList);
    argList.addAll(argListForAddingOneServer);

    final String[] args = argList.toArray(new String[defaultArgList.size() + argListForAddingOneServer.size()]);
    assertEquals("The job has been failed", LauncherStatus.COMPLETED, AddVectorET.runAddVector(args));
  }

  private List<String> getDefaultArguments(final int numWorkers, final int numServers) {
    final int numMiniBatches = 10;
    final int numWorkerBlocks = numWorkers * numMiniBatches;
    final int numDataPerWorker = numMiniBatches; // a single data item for a batch

    return Arrays.asList(
        "-max_num_epochs", Integer.toString(5),
        "-num_mini_batches", Integer.toString(numMiniBatches),
        "-num_worker_blocks", Integer.toString(numWorkerBlocks),
        "-num_server_blocks", Integer.toString(numServers),
        "-num_training_data", Integer.toString(numDataPerWorker),
        "-num_test_data", Integer.toString(10),
        "-delta", Integer.toString(4),
        "-num_keys", Integer.toString(50),
        "-input", ClassLoader.getSystemResource("data").getPath() + "/empty_file",
        "-vector_size", Integer.toString(5),
        "-compute_time_ms", Integer.toString(30),
        "-optimization_interval_ms", Integer.toString(3000),
        "-delay_after_optimization_ms", Integer.toString(10000),
        "-server_metric_flush_period_ms", Integer.toString(1000),
        "-timeout", Integer.toString(300000)
    );
  }
}
