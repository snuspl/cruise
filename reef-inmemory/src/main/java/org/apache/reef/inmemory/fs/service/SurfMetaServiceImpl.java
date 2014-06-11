package org.apache.reef.inmemory.fs.service;

import org.apache.hadoop.fs.Path;
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

import java.util.List;

public class SurfMetaServiceImpl implements SurfMetaService.Iface, SurfManagementService.Iface, Runnable, AutoCloseable {
  private int port = 18000;
  private int timeout = 30000;
  private int numThread = 10;

  TServer server = null;

  public SurfMetaServiceImpl(){
    this.port = 18000;
    this.timeout = 30000;
    this.numThread = 10;
  }
  @Override
  public List<FileMeta> listStatus(String path, boolean recursive, User user) throws FileNotFoundException, TException {
    SurfMetaManager sm = new SurfMetaManager();

    try {

      return sm.listStatus(new Path(path), recursive, user);
    } catch (java.io.FileNotFoundException fe) {
      throw new FileNotFoundException(fe.getMessage());
    }
  }

  @Override
  public FileMeta makeDirectory(String path, User user) throws FileAlreadyExistsException, TException {
    SurfMetaManager sm = new SurfMetaManager();

    try {
      return sm.makeDirectory(new Path(path), user);
    } catch (java.nio.file.FileAlreadyExistsException fe) {
      throw new FileAlreadyExistsException(fe.getMessage());
    }
  }

  @Override
  public long clear() throws TException {
    // TODO: implement clear message to caches
    System.out.println("Received clear message");
    return 0;
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
              .workerThreads(this.numThread));

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
