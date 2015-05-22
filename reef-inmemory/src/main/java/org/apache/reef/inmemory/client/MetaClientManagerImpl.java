package org.apache.reef.inmemory.client;

import org.apache.thrift.transport.TTransportException;

public class MetaClientManagerImpl implements MetaClientManager {

  @Override
  public MetaClientWrapper get(final String address) throws TTransportException {
    return new MetaClientWrapper(address);
  }
}
