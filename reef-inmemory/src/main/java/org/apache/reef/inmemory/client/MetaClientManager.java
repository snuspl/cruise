package org.apache.reef.inmemory.client;

import org.apache.thrift.transport.TTransportException;

/**
 * Manages Thrift client connections to the Meta Server.
 */
public interface MetaClientManager {
  /**
   * Instantiate a new metaClient and return the wrapper object.
   * The client is acquired via MetaClientWrapper.getClient().
   */
  public MetaClientWrapper get(String address) throws TTransportException;
}
