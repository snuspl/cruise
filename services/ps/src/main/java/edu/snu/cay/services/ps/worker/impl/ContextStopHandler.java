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
package edu.snu.cay.services.ps.worker.impl;

import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Closes the ParameterWorkerImpl.
 * This waits for the queued messages to be sent through NCS,
 * to avoid losing queued messages when closing a context (e.g., immediately after task completion).
 *
 * However, we are still observing some lost messages when
 * contexts are immediately closed after the task completes.
 * It appears messages buffered in NCS are not being flushed before context close,
 * but this has to be investigated further.
 * The current workaround is to not close contexts immediately, but this must be resolved before
 * we move to elastically removing contexts.
 */
public final class ContextStopHandler implements EventHandler<ContextStop> {
  private static final Logger LOG = Logger.getLogger(ContextStopHandler.class.getName());

  private ParameterWorkerImpl partitionedParameterWorker;

  @Inject
  private ContextStopHandler(final ParameterWorkerImpl partitionedParameterWorker) {
    this.partitionedParameterWorker = partitionedParameterWorker;
  }

  @Override
  public void onNext(final ContextStop contextStop) {
    LOG.log(Level.INFO, "Calling close. Will wait for close.");
    partitionedParameterWorker.close();
    LOG.log(Level.INFO, "Worker closed.");
  }
}
