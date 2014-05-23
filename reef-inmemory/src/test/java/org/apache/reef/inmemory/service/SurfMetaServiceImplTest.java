package org.apache.reef.inmemory.service;

import junit.framework.TestCase;
import org.apache.reef.inmemory.fs.entity.FileMeta;
import org.apache.reef.inmemory.fs.entity.User;
import org.apache.reef.inmemory.fs.service.SurfMetaService;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public class SurfMetaServiceImplTest extends TestCase {
    @Test
    public void testMakeDirectory() throws TTransportException {
        int port = 18000;
/**
        TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(port, 30000);
        SurfMetaService.Processor<SurfMetaService.Iface> processor = new SurfMetaService.Processor<SurfMetaService.Iface>(new SurfMetaServiceImpl());

        TServer server = new THsHaServer(
                new org.apache.thrift.server.THsHaServer.Args(
                        serverTransport).processor(processor)
                        .protocolFactory(new org.apache.thrift.protocol.TCompactProtocol.Factory())
                        .workerThreads(10));

        server.serve();
**/
    }

    public void testMakeDirectoryClient() throws Exception {
        /*
        TTransport transport = null;
        transport = new TFramedTransport(new TSocket("localhost", 18000,
                60000));
        TProtocol protocol = new TCompactProtocol(transport);
        transport.open();
        SurfMetaService.Client client = new SurfMetaService.Client(
                protocol);
        List<FileMeta> fms = client.listStatus("/user/surf", false, new User("surf", "surf"));

        for(FileMeta fm : fms){
            System.out.println(fm.getFullPath());
        }
        */
    }
}
