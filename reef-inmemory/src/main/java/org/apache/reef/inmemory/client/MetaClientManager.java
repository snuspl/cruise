package org.apache.reef.inmemory.client;

import org.apache.reef.inmemory.common.service.SurfMetaService;
import org.apache.thrift.transport.TTransportException;

/**
 * Manages Thrift client connections to the Meta Server.
 */
public interface MetaClientManager {
  /**
   * Instantiate and return a new metaClient
   */
  public SurfMetaService.Client get(String address) throws TTransportException;
}
