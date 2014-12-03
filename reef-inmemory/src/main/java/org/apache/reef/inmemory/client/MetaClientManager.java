package org.apache.reef.inmemory.client;

import org.apache.reef.inmemory.common.service.SurfMetaService;
import org.apache.thrift.transport.TTransportException;

public interface MetaClientManager {
  public SurfMetaService.Client get(String address) throws TTransportException;
}
