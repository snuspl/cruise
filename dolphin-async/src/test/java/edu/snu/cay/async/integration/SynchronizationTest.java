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
package edu.snu.cay.async.integration;

import edu.snu.cay.async.AsyncDolphinConfiguration;
import edu.snu.cay.async.AsyncDolphinLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.parameters.EvaluatorFailedHandlers;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.EventHandler;
import org.junit.Test;

import javax.inject.Inject;

import static org.junit.Assert.assertEquals;

/**
 * An integration test for SynchronizationManager and WorkerSynchronizer.
 */
public final class SynchronizationTest {

  /**
   * In SynchronizationUpdater#process, the synchronization feature is tested using the messages from
   * SynchronizationWorker.
   */
  @Test
  public void testSynchronization() {
    final String[] args = {
        "-maxIter", "5",
        "-maxNumEvalLocal", "9",
        "-split", "8",
        "-input", ClassLoader.getSystemResource("data").getPath() + "/empty_file",
        "-numWorkerThreads", "3"
    };

    final LauncherStatus status = AsyncDolphinLauncher.launch("SynchronizationTest", args,
        AsyncDolphinConfiguration.newBuilder()
            .setKeyCodecClass(SerializableCodec.class)
            .setPreValueCodecClass(SerializableCodec.class)
            .setValueCodecClass(SerializableCodec.class)
            .setUpdaterClass(SynchronizationTestUpdater.class)
            .setWorkerClass(SynchronizationTestWorker.class)
            .build(),
        Tang.Factory.getTang().newConfigurationBuilder()
            .bindSetEntry(EvaluatorFailedHandlers.class, EvaluatorFailedHandler.class)
            .build());
    assertEquals(status, LauncherStatus.COMPLETED);
  }

  /**
   * Propagates an exception by throwing RuntimeException in the driver
   * when the server node was killed from test failure.
   */
  public static final class EvaluatorFailedHandler implements EventHandler<FailedEvaluator> {

    @Inject
    private EvaluatorFailedHandler() {
    }

    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      throw new RuntimeException("Test failed");
    }
  }
}
