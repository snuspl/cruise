package org.apache.reef.inmemory.client;

import com.google.common.net.HostAndPort;
import org.apache.reef.inmemory.common.service.SurfCacheService;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.HashMap;
import java.util.Map;

/**
 * Manages Thrift client connections to the Cache nodes.
 * Client connections are created and then left open.
 */
public interface CacheClientManager {
  /**
   * Client repeatedly try to connect when failure occurred.
   * @return The number of retries Client makes up to.
   */
  public int getRetries();

  /**
   * Client retries to the Cache node after this time interval.
   * @return The time interval to wait after failure.
   */
  public int getRetriesInterval();

  /**
   * @return The size of buffer Client uses.
   */
  public int getBufferSize();

  /**
   * Returns a client for the given address. The caller of this method must ensure that
   * operations on the client are wrapped in a synchronized (client) block.
   * @param address The address of Cache node.
   * @return The Client connected to the cache node.
   */
  public SurfCacheService.Client get(String address) throws TTransportException;
}
