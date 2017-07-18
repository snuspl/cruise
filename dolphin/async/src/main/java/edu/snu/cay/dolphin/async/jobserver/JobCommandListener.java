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

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Logger;

/**
 * It receives job command from {@link JobCommandSender} and directly sends it to {@link JobServerDriver}
 * via client message channel.
 */
public final class JobCommandListener implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(JobCommandListener.class.getName());

  private volatile RunningJob reefJob;
  private volatile boolean isClosed = false;

  @Inject
  private JobCommandListener() throws IOException {
    // single thread is enough
    new Thread(() -> {
      try (ServerSocket serverSocket = new ServerSocket(Parameters.PORT_NUMBER)) {
        while (!isClosed) {
          try (Socket socket = serverSocket.accept();
               BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
               PrintWriter pw = new PrintWriter(socket.getOutputStream())) {
            final String input = br.readLine();
            final String command = input.split(" ")[0];
            if (reefJob != null) {
              reefJob.send(input.getBytes());

              pw.println("Job command received in JobCommandListener : " + command);
              pw.flush();
            } else {
              pw.println("JobServer is not ready yet");
              pw.flush();
            }
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }).start();
  }

  @Override
  public void close() throws Exception {
    isClosed = true;
  }

  /**
   * Registers REEF job to send client message.
   * When it receives transport event from other sources, it passes messages to registered job.
   */
  void setReefJob(final RunningJob reefJob) {
    this.reefJob = reefJob;
  }
}
