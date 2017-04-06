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
package edu.snu.cay.utils;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Resolves the hostname of the machine which the JVM process runs on.
 */
public final class HostnameResolver {
  private static final Logger LOG = Logger.getLogger(HostnameResolver.class.getName());
  private static final String EMPTY = "";

  /**
   * Utility class should not be instantiated.
   */
  private HostnameResolver() {
  }

  /**
   * @return the hostname if found, and return an empty string when failed.
   */
  public static String resolve() {
    try {
      return Inet4Address.getLocalHost().getHostName();
    } catch (final UnknownHostException e) {
      LOG.log(Level.WARNING, "Failed to get the local hostname");
    }

    return EMPTY;
  }
}
