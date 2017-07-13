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

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.impl.LoggingEventHandler;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.Link;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.TransportFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

/**
 * Sender class of {@link JobLauncher}.
 * It sends job command and serialized job configuration to {@link JobServerClient}.
 */
final class JobCommandSender {

  private static final Logger LOG = Logger.getLogger(JobCommandSender.class.getName());
  private final TransportFactory tpFactory;
  private final String address;
  private final transient int port;

  @Inject
  private JobCommandSender(final TransportFactory tpFactory,
                           @Parameter(Parameters.Address.class) final String address,
                           @Parameter(Parameters.Port.class) final int port) {
    this.tpFactory = tpFactory;
    this.address = address;
    this.port = port;
  }

  void sendJobCommand(final String command, final String serializedConf) throws Exception {
    final ObjectSerializableCodec<String> codec = new ObjectSerializableCodec<>();

    final EStage<TransportEvent> stage = new ThreadPoolStage<>("JobServer",
        new LoggingEventHandler<TransportEvent>(), 1, throwable -> {
      throw new RuntimeException(throwable);
    });

    try (Transport transport = tpFactory.newInstance(address, 0, stage, stage, 1, 10000)) {
      final Link<String> link = transport.open(new InetSocketAddress(address, port), codec, null);
      final String message = command + " " + serializedConf;
      link.write(message);
    }
  }
}
