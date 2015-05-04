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


public final class CacheClientManagerImpl implements CacheClientManager {
  private final Map<String, SurfCacheService.Client> cacheClients = new HashMap<>();

  private final int retries;
  private final int retriesInterval;
  private final int bufferSize;

  public CacheClientManagerImpl(final int retries,
                                final int retriesInterval,
                                final int bufferSize) {
    this.retries = retries;
    this.retriesInterval = retriesInterval;
    this.bufferSize = bufferSize;
  }

  @Override
  public int getRetries() {
    return retries;
  }

  @Override
  public int getRetriesInterval() {
    return retriesInterval;
  }

  @Override
  public int getBufferSize() {
    return bufferSize;
  }

  private static SurfCacheService.Client create(final String address) throws TTransportException {
    final HostAndPort taskAddress = HostAndPort.fromString(address);

    final TTransport transport = new TFramedTransport(new TSocket(taskAddress.getHostText(), taskAddress.getPort()));
    transport.open();
    final TProtocol protocol = new TCompactProtocol(transport);
    return new SurfCacheService.Client(protocol);
  }

  @Override
  public synchronized SurfCacheService.Client get(final String address) throws TTransportException {
    if (!cacheClients.containsKey(address)) {
      cacheClients.put(address, create(address));
    }
    return cacheClients.get(address);
  }
}
