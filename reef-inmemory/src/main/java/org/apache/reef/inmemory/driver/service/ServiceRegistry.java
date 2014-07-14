package org.apache.reef.inmemory.driver.service;

public interface ServiceRegistry {
  void register(String localHostAddress, int servicePort);
}
