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
package edu.snu.cay.dolphin.async.metric.dashboard;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Launcher class for Dashboard.
 */
public final class DashboardLauncher {
  private static final Logger LOG = Logger.getLogger(DashboardLauncher.class.getName());

  private static final String DASHBOARD_DIR = "/dashboard";
  private static final String DASHBOARD_SCRIPT = "dashboard.py";

  // Utility class should not be instantiated
  private DashboardLauncher() {

  }

  /**
   * Copy the server launching script directory to java tmpdir and run Dashboard server on localhost.
   * @param port The port number.
   * @throws IOException when the given port is unavailable, failed to copy the server script,
   *           or failed to make processBuilder.
   */
  public static void runDashboardServer(final int port) throws IOException {
    LOG.log(Level.INFO, "Now launch dashboard server");

    // Check if the port number is available by trying to connect a socket to given port.
    // If the connection is successful, it means the port number is already in use.
    // Only when connectionException occurs means that the port number is available.
    try {
      final String loopbackAddr = null;
      (new Socket(loopbackAddr, port)).close();
      LOG.log(Level.WARNING, "Port number already in use.");
      throw new IOException("Port number already in use.");
    } catch (ConnectException connectException) {
      // expected exception
    }

    final Path tmpPath = Paths.get(System.getProperty("java.io.tmpdir"), DASHBOARD_DIR);
    // Copy the dashboard python script to /tmp
    try {
      final FileSystem fileSystem = FileSystems.newFileSystem(
          DashboardLauncher.class.getResource("").toURI(),
          Collections.<String, String>emptyMap()
      );

      final Path jarPath = fileSystem.getPath(DASHBOARD_DIR);
      Files.walkFileTree(jarPath, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
          Files.createDirectories(tmpPath.resolve(jarPath.relativize(dir).toString()));
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
          Files.copy(file, tmpPath.resolve(jarPath.relativize(file).toString()), StandardCopyOption.REPLACE_EXISTING);
          return FileVisitResult.CONTINUE;
        }
      });

      // Launch server
      final String tmpScript = Paths.get(tmpPath.toString(), DASHBOARD_SCRIPT).toString();
      final ProcessBuilder pb = new ProcessBuilder("python", tmpScript, String.valueOf(port)).inheritIO();

      final Process p = pb.start();
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          p.destroy();
        }
      });
    } catch (URISyntaxException e) {
      LOG.log(Level.WARNING, "Failed to access current jar file.", e);
    }
  }
}
