package org.apache.reef.inmemory.driver.service;

/**
 * Used to register the MetaServer's host and port to a naming service.
 */
public interface ServiceRegistry {
  void register(String hostAddress, int servicePort);
}
