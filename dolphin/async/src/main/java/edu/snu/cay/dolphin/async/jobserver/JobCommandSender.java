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


import javax.inject.Inject;
import java.io.*;
import java.net.Socket;
import java.util.logging.Logger;

/**
 * A class for sending a job command message from {@link JobLauncher} to {@link JobServerClient}.
 * {@link JobCommandListener} at {@link JobServerClient} will receive the message.
 * For job submission command, a message contains a serialized job configuration.
 */
final class JobCommandSender {

  private static final Logger LOG = Logger.getLogger(JobCommandSender.class.getName());
  private static final String EMPTY_JOB_CONF = "empty";

  @Inject
  private JobCommandSender() {
  }

  /**
   * Sends a job submit command message to {@link JobServerClient}.
   * Client will pass command message to {@link JobServerDriver} to call
   * a method {@code JobServerDrier.executeJob(String)}.
   * @param serializedConf a serialized job configuration.
   */
  void sendJobSubmitCommand(final String serializedConf) throws IOException {
    final String commandMsg = Parameters.SUBMIT_COMMAND + " " + serializedConf;
    sendCommand(commandMsg);
  }

  /**
   * Sends a shut down command message to {@link JobServerClient}.
   * Client will pass command message to {@link JobServerDriver} to call a method {@code JobServerDriver.shutdown()}.
   */
  void sendShutdownCommand() throws IOException {
    final String commandMsg = Parameters.SHUTDOWN_COMMAND + " " + EMPTY_JOB_CONF;
    sendCommand(commandMsg);
  }

  /**
   * Sends command message using standard java socket network channel.
   */
  private void sendCommand(final String command) throws IOException {
    try (Socket socket = new Socket("localhost", Parameters.PORT_NUMBER);
         BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
         PrintWriter pw = new PrintWriter(socket.getOutputStream())) {
      pw.println(command);
      pw.flush();

      final String response = br.readLine();
      System.out.println(response);
    }
  }
}
