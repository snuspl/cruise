package org.apache.reef.inmemory.cache.service;

import com.microsoft.tang.annotations.Parameter;
import org.apache.reef.inmemory.cache.CacheParameters;
import org.apache.reef.inmemory.cache.InMemoryCache;
import org.apache.reef.inmemory.cache.hdfs.HdfsBlockId;
import org.apache.reef.inmemory.fs.entity.BlockInfo;
import org.apache.reef.inmemory.fs.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.fs.exceptions.BlockNotFoundException;
import org.apache.reef.inmemory.fs.service.SurfCacheService;
import org.apache.thrift.TException;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

/**
 * Serves block data in cache to Surf clients, using Thrift.
 */
public final class SurfCacheServer implements SurfCacheService.Iface, Runnable, AutoCloseable {

  private static final Logger LOG = Logger.getLogger(SurfCacheServer.class.getName());

  private final InMemoryCache cache;
  private final int port;
  private final int timeout;
  private final int numThreads;
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

      SurfCacheService.Processor<SurfCacheService.Iface> processor =
              new SurfCacheService.Processor<SurfCacheService.Iface>(this);

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
  public ByteBuffer getData(final BlockInfo block) throws BlockNotFoundException, BlockLoadingException, TException {
    HdfsBlockId blockId = HdfsBlockId.copyBlock(block);
    return ByteBuffer.wrap(cache.get(blockId));
  }
}
