package org.apache.reef.inmemory.cache.service;

import com.microsoft.tang.annotations.Parameter;
import org.apache.reef.inmemory.cache.CacheParameters;
import org.apache.reef.inmemory.cache.InMemoryCache;
import org.apache.reef.inmemory.cache.hdfs.HdfsBlockId;
import org.apache.reef.inmemory.fs.entity.BlockInfo;
import org.apache.reef.inmemory.fs.exceptions.FileNotFoundException;
import org.apache.reef.inmemory.fs.service.SurfCacheService;
import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

public final class SurfCacheServer implements SurfCacheService.Iface, Runnable, AutoCloseable {

  private static final Logger LOG = Logger.getLogger(SurfCacheServer.class.getName());

  private final InMemoryCache cache;
  private final int port;
  private final int timeout;
  private final int numThreads; // TODO: this has to be moved out to parameters only for the server
  TServer server = null;

  @Inject
  public SurfCacheServer(final InMemoryCache cache,
                         final @Parameter(CacheParameters.Port.class) int port,
                         final @Parameter(CacheParameters.Timeout.class) int timeout,
                         final @Parameter(CacheParameters.NumThreads.class) int numThreads) {
    this.cache = cache;
    this.port = port;
    this.timeout = timeout;
    this.numThreads = numThreads;
  }

  @Override
  public void run() {
    try {
      TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(this.port, this.timeout);

      TMultiplexedProcessor processor = new TMultiplexedProcessor();
      SurfCacheService.Processor<SurfCacheService.Iface> metaProcessor =
              new SurfCacheService.Processor<SurfCacheService.Iface>(this);
      processor.registerProcessor(SurfCacheService.class.getName(), metaProcessor);

      this.server = new THsHaServer(
          new THsHaServer.Args(serverTransport).processor(processor)
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

  @Override
  public ByteBuffer getData(BlockInfo block) throws FileNotFoundException, TException {
    HdfsBlockId blockId = HdfsBlockId.copyBlock(block);
    byte[] data = cache.get(blockId);
    if (data == null) {
      throw new FileNotFoundException();
    } else {
      return ByteBuffer.wrap(data);
    }
  }
}
