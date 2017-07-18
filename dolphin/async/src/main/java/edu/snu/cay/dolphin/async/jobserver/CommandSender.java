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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class for sending a job command message from {@link JobLauncher} to {@link JobServerClient}.
 * {@link CommandListener} at {@link JobServerClient} will receive the message.
 * For job submission command, a message contains a serialized job configuration.
 */
final class CommandSender {

  private static final Logger LOG = Logger.getLogger(CommandSender.class.getName());

  @Inject
  private CommandSender() {
  }

  /**
   * Sends a job submit command message to {@link JobServerClient}.
   * Client will pass command message to {@link JobServerDriver} to call
   * a method {@code JobServerDrier.executeJob(String)}.
   * @param serializedConf a serialized job configuration.
   */
  void sendJobSubmitCommand(final String serializedConf) throws IOException {
    final String commandMsg = Parameters.SUBMIT_COMMAND + Parameters.COMMAND_DELIMITER + serializedConf;

    LOG.log(Level.INFO, "Job command : {0}", new Object[]{Parameters.SUBMIT_COMMAND});
    sendCommand(commandMsg);
  }

  /**
   * Sends a shut down command message to {@link JobServerClient}.
   * Client will pass command message to {@link JobServerDriver} to call a method {@code JobServerDriver.shutdown()}.
   */
  void sendShutdownCommand() throws IOException {
    final String commandMsg = Parameters.SHUTDOWN_COMMAND + Parameters.COMMAND_DELIMITER;

    LOG.log(Level.INFO, "Job command : {0}", new Object[]{Parameters.SHUTDOWN_COMMAND});
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

      final boolean response = Boolean.parseBoolean(br.readLine());

      if (!response) {
        throw new IOException("Fail to send command");
      }
    }
  }
}
