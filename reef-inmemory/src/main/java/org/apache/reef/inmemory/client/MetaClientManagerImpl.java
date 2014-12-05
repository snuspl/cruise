package org.apache.reef.inmemory.client;

import com.google.common.net.HostAndPort;
import org.apache.reef.inmemory.common.service.SurfMetaService;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class MetaClientManagerImpl implements MetaClientManager {
  public SurfMetaService.Client get(final String address) throws TTransportException {
    final HostAndPort metaAddress = HostAndPort.fromString(address);
    final TTransport transport = new TFramedTransport(new TSocket(metaAddress.getHostText(), metaAddress.getPort()));
    transport.open();
    final TProtocol protocol = new TMultiplexedProtocol(
        new TCompactProtocol(transport),
        SurfMetaService.class.getName());
    return new SurfMetaService.Client(protocol);
  }
}
