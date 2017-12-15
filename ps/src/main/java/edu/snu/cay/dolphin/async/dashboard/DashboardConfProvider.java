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
package edu.snu.cay.dolphin.async.dashboard;

import edu.snu.cay.dolphin.async.dashboard.parameters.DashboardHostAddress;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;

import java.net.*;
import java.util.Enumeration;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Configuration provider for Dashboard.
 */
public final class DashboardConfProvider {
  private static final Logger LOG = Logger.getLogger(DashboardConfProvider.class.getName());

  // Utility class should not be instantiated
  private DashboardConfProvider() {
  }

  public static Configuration getConfiguration(final boolean dashboardEnabled) {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    if (dashboardEnabled) {
      confBuilder.bindNamedParameter(DashboardHostAddress.class, getHostAddress());
    }
    return confBuilder.build();
  }

  /**
   * Find the Host address of Client machine.
   * @return String of HostAddress.
   */
  private static String getHostAddress() {
    // Find IP address of client PC.
    try {
      final Enumeration e = NetworkInterface.getNetworkInterfaces();
      while (e.hasMoreElements()) {
        final NetworkInterface n = (NetworkInterface) e.nextElement();
        if (n.isLoopback() || n.isVirtual() || !n.isUp()) {
          continue;
        }
        final Enumeration ee = n.getInetAddresses();
        while (ee.hasMoreElements()) {
          final InetAddress i = (InetAddress) ee.nextElement();
          if (i.isLinkLocalAddress()) {
            continue;
          }

          final String hostAddress = i.getHostAddress();
          LOG.log(Level.INFO, "URL found: {0}", hostAddress);
          return hostAddress;
        }
      }
      throw new RuntimeException("Fail to find host address");
    } catch (final SocketException e) {
      throw new RuntimeException("Fail to find host address", e);
    }
  }
}
