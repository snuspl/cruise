package org.apache.reef.inmemory.client;

import com.google.common.net.HostAndPort;
import org.apache.reef.inmemory.fs.service.SurfCacheService;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
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
public final class CacheClientManager {
  private final Map<String, SurfCacheService.Client> cacheClients = new HashMap<>();

  private final int retries;
  private final int retriesInterval;

  public CacheClientManager(int retries, int retriesInterval) {
    this.retries = retries;
    this.retriesInterval = retriesInterval;
  }

  public int getRetries() {
    return retries;
  }

  public int getRetriesInterval() {
    return retriesInterval;
  }

  private static SurfCacheService.Client create(String address)
          throws TTransportException {
    HostAndPort taskAddress = HostAndPort.fromString(address);

    TTransport transport = new TFramedTransport(new TSocket(taskAddress.getHostText(), taskAddress.getPort()));
    transport.open();
    TProtocol protocol = new TMultiplexedProtocol(
            new TCompactProtocol(transport),
            SurfCacheService.class.getName());
    return new SurfCacheService.Client(protocol);
  }

  /**
   * Returns a client for the given address. The caller of this method must ensure that
   * operations on the client are wrapped in a synchronized (client) block.
   */
  public synchronized SurfCacheService.Client get(String address) throws TTransportException {
    if (!cacheClients.containsKey(address)) {
      cacheClients.put(address, create(address));
    }
    return cacheClients.get(address);
  }
}
