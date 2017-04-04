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

  /**
   * Utility class should not be instantiated.
   */
  private HostnameResolver() {
  }

  /**
   * @return the hostname if found, and return {@code null} when failed.
   */
  public static String resolve() {
    try {
      return Inet4Address.getLocalHost().getHostName();
    } catch (final UnknownHostException e) {
      LOG.log(Level.WARNING, "Failed to get the local hostname");
    }

    return null;
  }
}
