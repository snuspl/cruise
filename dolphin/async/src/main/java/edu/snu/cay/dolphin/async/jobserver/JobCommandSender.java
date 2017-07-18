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

import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.wake.remote.transport.TransportFactory;

import javax.inject.Inject;
import java.io.*;
import java.net.Socket;
import java.util.logging.Logger;

/**
 * Sender class of {@link JobLauncher}.
 * It sends job command and serialized job configuration to {@link JobServerClient}.
 */
final class JobCommandSender {

  private static final Logger LOG = Logger.getLogger(JobCommandSender.class.getName());
  private static final String EMPTY_JOB_CONF = "empty";
  private final TransportFactory tpFactory;
  private final ObjectSerializableCodec<String> codec;

  @Inject
  private JobCommandSender(final TransportFactory tpFactory) {
    this.tpFactory = tpFactory;
    this.codec = new ObjectSerializableCodec<>();
  }

  /**
   * Sends a job submit command message to {@link JobServerClient}.
   * Client will pass command message to {@link JobServerDriver} to call
   * a method {@code JobServerDrier.executeJob(String)}.
   * @param serializedConf a serialized job configuration.
   */
  void sendJobCommand(final String serializedConf) throws Exception {
    /*
    final EStage<TransportEvent> stage = new ThreadPoolStage<>("JobServer",
        new LoggingEventHandler<TransportEvent>(), 1, throwable -> {
      throw new RuntimeException(throwable);
    });

    try (Transport transport = tpFactory.newInstance("localhost", 0, stage, stage, 1, 10000)) {
      final Link<String> link = transport.open(new InetSocketAddress("localhost", Parameters.PORT_NUMBER), codec, null);
      final String message = Parameters.SUBMIT_COMMAND + " " + serializedConf;
      link.write(message);
    }
    */

    final Socket socket = new Socket("localhost", Parameters.PORT_NUMBER);
    final BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    final PrintWriter pw = new PrintWriter(socket.getOutputStream());

    try {
      pw.println(Parameters.SUBMIT_COMMAND + " " + serializedConf);
      pw.flush();
      System.out.println(br.readLine());

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      try {
        socket.close();
        br.close();
        pw.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Sends a shut down command message to {@link JobServerClient}.
   * Client will pass command message to {@link JobServerDriver} to call a method {@code JobServerDriver.shutdown()}.
   */
  void shutdown() throws Exception {
    /*
    final EStage<TransportEvent> stage = new ThreadPoolStage<>("JobServer",
        new LoggingEventHandler<TransportEvent>(), 1, throwable -> {
      throw new RuntimeException(throwable);
    });

    try (Transport transport = tpFactory.newInstance("localhost", 0, stage, stage, 1, 10000)) {
      final Link<String> link = transport.open(new InetSocketAddress("localhost", Parameters.PORT_NUMBER), codec, null);
      final String message = Parameters.SHUTDOWN_COMMAND + " " + EMPTY_JOB_CONF;
      link.write(message);
    }
    */

    final Socket socket = new Socket("localhost", Parameters.PORT_NUMBER);
    final BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    final PrintWriter pw = new PrintWriter(socket.getOutputStream());

    try {
      pw.println(Parameters.SHUTDOWN_COMMAND + " " + EMPTY_JOB_CONF);
      pw.flush();
      System.out.println(br.readLine());

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      try {
        socket.close();
        br.close();
        pw.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
