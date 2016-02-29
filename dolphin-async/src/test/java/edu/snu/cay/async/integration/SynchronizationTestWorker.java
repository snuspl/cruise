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

import edu.snu.cay.async.Worker;
import edu.snu.cay.async.WorkerSynchronizer;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;

import javax.inject.Inject;
import java.util.Random;

final class SynchronizationTestWorker implements Worker {

  static final String BEFORE_BARRIER_MSG = "BEFORE_BARRIER_MSG";
  static final String AFTER_BARRIER_MSG = "AFTER_BARRIER_MSG";

  private int currentIter;
  private final ParameterWorker<Integer, String, String> parameterWorker;
  private final WorkerSynchronizer synchronizer;
  private final Random random;

  @Inject
  private SynchronizationTestWorker(final ParameterWorker<Integer, String, String> parameterWorker,
                                    final WorkerSynchronizer synchronizer) {
    this.parameterWorker = parameterWorker;
    this.synchronizer = synchronizer;
    this.random = new Random();
  }

  @Override
  public void initialize() {
    synchronizer.globalBarrier();
  }

  @Override
  public void run() {
    parameterWorker.push(currentIter, BEFORE_BARRIER_MSG);
    try {
      Thread.sleep(random.nextInt(500));
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
    synchronizer.globalBarrier();
    parameterWorker.push(currentIter, AFTER_BARRIER_MSG);
    currentIter++;
  }

  @Override
  public void cleanup() {
    synchronizer.globalBarrier();
  }
}
