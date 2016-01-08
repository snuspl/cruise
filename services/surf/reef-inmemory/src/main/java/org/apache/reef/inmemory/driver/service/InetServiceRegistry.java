package org.apache.reef.inmemory.driver.service;

import javax.inject.Inject;

/**
 * Implementation to be used when a naming service is not required,
 * because the raw address is a well-known location.
 */
public final class InetServiceRegistry implements ServiceRegistry {

  @Inject
  public InetServiceRegistry() {
  }

  @Override
  public void register(String host, int port) {
    // Do Nothing
  }
}
