package org.apache.reef.inmemory.client;

import org.apache.reef.inmemory.common.service.SurfCacheService;
import org.apache.thrift.transport.TTransportException;

/**
 * Manages Thrift client connections to the Cache nodes.
 * Client connections are created and then left open.
 */
public interface CacheClientManager {
  /**
   * Client repeatedly try to connect when failure occurred.
   * @return The number of retries Client makes up to.
   */
  int getRetries();

  /**
   * Client retries to the Cache node after this time interval.
   * @return The time interval to wait after failure.
   */
  int getRetriesInterval();

  /**
   * @return The size of buffer Client uses.
   */
  int getBufferSize();

  /**
   * Returns a client for the given address. The caller of this method must ensure that
   * operations on the client are wrapped in a synchronized (client) block.
   * @param address The address of Cache node.
   * @return The Client connected to the cache node.
   */
  SurfCacheService.Client get(String address) throws TTransportException;
}
