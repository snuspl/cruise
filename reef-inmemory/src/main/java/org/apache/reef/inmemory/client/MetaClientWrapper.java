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

/**
 * Wrap the Meta Client to release the resources properly when the connection is finished.
 */
public class MetaClientWrapper implements AutoCloseable {
  private final SurfMetaService.Client client;
  private final TTransport transport;

  public MetaClientWrapper(final String address) throws TTransportException {
    final HostAndPort metaAddress = HostAndPort.fromString(address);
    transport = new TFramedTransport(new TSocket(metaAddress.getHostText(), metaAddress.getPort()));
    transport.open();
    final TProtocol protocol = new TMultiplexedProtocol(new TCompactProtocol(transport), SurfMetaService.class.getName());
    this.client = new SurfMetaService.Client(protocol);
  }

  public SurfMetaService.Client getClient() {
    return client;
  }

  @Override
  public void close() throws Exception {
    transport.close(); // The Socket is also closed internally.
  }
}
