package org.apache.reef.inmemory.fs.service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.microsoft.tang.annotations.Parameter;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.Launch;
import org.apache.reef.inmemory.fs.SurfMetaManager;
import org.apache.reef.inmemory.fs.entity.FileMeta;
import org.apache.reef.inmemory.fs.entity.User;
import org.apache.reef.inmemory.fs.exceptions.FileAlreadyExistsException;
import org.apache.reef.inmemory.fs.exceptions.FileNotFoundException;
import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

public class SurfMetaServiceImpl implements SurfMetaService.Iface, SurfManagementService.Iface, Runnable, AutoCloseable {
  private final int port;
  private final int timeout;
  private final int numThreads;
  TServer server = null;

  private final SurfMetaManager metaManager;

  @Inject
  public SurfMetaServiceImpl(final SurfMetaManager metaManager,
                             final @Parameter(MetaServerParameters.Port.class) int port,
                             final @Parameter(MetaServerParameters.Timeout.class) int timeout,
                             final @Parameter(MetaServerParameters.Threads.class) int numThreads) {
    this.metaManager = metaManager;

    this.port = port;
    this.timeout = timeout;
    this.numThreads = numThreads;
  }

  @Override
  public List<FileMeta> listStatus(String path, boolean recursive, User user) throws FileNotFoundException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileMeta makeDirectory(String path, User user) throws FileAlreadyExistsException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long clear() throws TException {
    return metaManager.clear();
  }

  @Override
  public void run() {
    try {
      TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(this.port, this.timeout);

      TMultiplexedProcessor processor = new TMultiplexedProcessor();
      SurfMetaService.Processor<SurfMetaService.Iface> metaProcessor =
              new SurfMetaService.Processor<SurfMetaService.Iface>(this);
      processor.registerProcessor(SurfMetaService.class.getName(), metaProcessor);
      SurfManagementService.Processor<SurfManagementService.Iface> managementProcessor =
              new SurfManagementService.Processor<SurfManagementService.Iface>(this);
      processor.registerProcessor(SurfManagementService.class.getName(), managementProcessor);

      this.server = new THsHaServer(
          new org.apache.thrift.server.THsHaServer.Args(serverTransport).processor(processor)
              .protocolFactory(new org.apache.thrift.protocol.TCompactProtocol.Factory())
              .workerThreads(this.numThreads));

      this.server.serve();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (this.server != null && this.server.isServing())
        this.server.stop();
    }
  }

  @Override
  public void close() throws Exception {
    if (this.server != null && this.server.isServing())
      this.server.stop();
  }
}
