package org.apache.reef.inmemory.fs.service;

import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.fs.SurfMetaManager;
import org.apache.reef.inmemory.fs.entity.FileMeta;
import org.apache.reef.inmemory.fs.entity.User;
import org.apache.reef.inmemory.fs.exceptions.FileAlreadyExistsException;
import org.apache.reef.inmemory.fs.exceptions.FileNotFoundException;
import org.apache.thrift.TException;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;

import java.util.ArrayList;
import java.util.List;

public class SurfMetaServiceImpl implements SurfMetaService.Iface, Runnable {
  @Override
  public List<FileMeta> listStatus(String path, boolean recursive, User user) throws FileNotFoundException, TException {
    SurfMetaManager sm = new SurfMetaManager();

    List<FileMeta> fms = new ArrayList<FileMeta>();
    FileMeta fm = new FileMeta();
    fm.setFullPath("/user/surf");
    fms.add(fm);

    return fms;
//
//        try{
//
//            return sm.listStatus(new Path(path), recursive, user);
//        }catch(java.io.FileNotFoundException fe){
//            throw new FileNotFoundException(fe.getMessage());
//        }
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
  public void run() {
    try {
      int port = 18000;

      TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(port, 30000);
      SurfMetaService.Processor<SurfMetaService.Iface> processor = new SurfMetaService.Processor<SurfMetaService.Iface>(new SurfMetaServiceImpl());

      TServer server = new THsHaServer(
          new org.apache.thrift.server.THsHaServer.Args(serverTransport).processor(processor)
              .protocolFactory(new org.apache.thrift.protocol.TCompactProtocol.Factory())
              .workerThreads(10));

      server.serve();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
