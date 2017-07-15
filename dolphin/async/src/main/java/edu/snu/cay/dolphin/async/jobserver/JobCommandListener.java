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

import org.apache.reef.client.RunningJob;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.TransportFactory;

import javax.inject.Inject;
import java.util.logging.Logger;

/**
 * It receives job command from {@link JobCommandSender}, converts it to client message
 * for sending to the {@link JobServerDriver}.
 */
public final class JobCommandListener implements EventHandler<TransportEvent>, AutoCloseable {

  private static final Logger LOG = Logger.getLogger(JobCommandListener.class.getName());
  private final transient Transport transport;
  private transient RunningJob reefJob;

  @Inject
  private JobCommandListener(final TransportFactory tpFactory,
                             @Parameter(Parameters.Address.class) final String address,
                             @Parameter(Parameters.Port.class) final int port) {
    final EStage<TransportEvent> stage = new ThreadPoolStage<>(
        "JobServer", this, 1, throwable -> {
      throw new RuntimeException(throwable);
    });
    transport = tpFactory.newInstance(address, port, stage, stage, 1, 10000);
  }

  @Override
  public void onNext(final TransportEvent transportEvent) {
    if (reefJob != null) {
      reefJob.send(transportEvent.getData());
    }
  }

  @Override
  public void close() throws Exception {
    transport.close();
  }

  /**
   * Registers REEF job to send client message.
   * When it receives transport event from other sources, it passes messages to registered job.
   */
  void setReefJob(final RunningJob reefJob) {
    this.reefJob = reefJob;
  }

}
