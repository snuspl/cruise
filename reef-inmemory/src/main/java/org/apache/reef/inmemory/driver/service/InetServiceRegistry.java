package org.apache.reef.inmemory.driver.service;

/**
 * Implementation to be used when a naming service is not required,
 * because the raw address is a well-known location.
 */
public final class InetServiceRegistry implements ServiceRegistry {
  @Override
  public void register(String host, int port) {
    // Do Nothing
  }
}
