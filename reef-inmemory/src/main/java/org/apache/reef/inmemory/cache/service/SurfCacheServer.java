package org.apache.reef.inmemory.cache.service;

import com.microsoft.tang.annotations.Parameter;
import org.apache.reef.inmemory.cache.CacheParameters;
import org.apache.reef.inmemory.cache.InMemoryCache;
import org.apache.reef.inmemory.cache.hdfs.HdfsBlockId;
import org.apache.reef.inmemory.fs.entity.BlockInfo;
import org.apache.reef.inmemory.fs.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.fs.exceptions.BlockNotFoundException;
import org.apache.reef.inmemory.fs.service.SurfCacheService;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;

import javax.inject.Inject;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.logging.Level;
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

  private TServer server = null;
  private int bindPort;

  @Inject
  public SurfCacheServer(final InMemoryCache cache,
                         final @Parameter(CacheParameters.Port.class) int port,
                         final @Parameter(CacheParameters.Timeout.class) int timeout,
                         final @Parameter(CacheParameters.NumServerThreads.class) int numThreads) {
    this.cache = cache;
    this.port = port;
    this.timeout = timeout;
    this.numThreads = numThreads;
  }

  public int getBindPort() {
    return this.bindPort;
  }

  /**
   * Initialize the port to bind. If cache port is 0, will return a new ephemeral port.
   * If cache port is specified, it will return the port.
   *
   * The ephemeral port is not guaranteed to be open when run is called.
   * But must do this because Thrift will not return a bound port number.
   */
  public int initBindPort() throws IOException {
    if (this.port == 0) {
      ServerSocket reservation = new ServerSocket(0, 1);
      this.bindPort = reservation.getLocalPort();
      reservation.close();
    } else {
      this.bindPort = this.port;
    }
    return this.bindPort;
  }

  @Override
  public void run() {
    try {
      TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(this.bindPort, this.timeout);

      SurfCacheService.Processor<SurfCacheService.Iface> processor =
              new SurfCacheService.Processor<SurfCacheService.Iface>(this);

      this.server = new THsHaServer(
          new THsHaServer.Args(serverTransport).processor(processor)
              .protocolFactory(new org.apache.thrift.protocol.TCompactProtocol.Factory())
              .workerThreads(this.numThreads));

      this.server.serve();
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Exception while serving "+e);
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
  public ByteBuffer getData(final BlockInfo blockInfo,
                           final long offset, final long length) throws BlockNotFoundException, BlockLoadingException {
    HdfsBlockId blockId = HdfsBlockId.copyBlock(blockInfo);

    byte[] block = cache.get(blockId);
    ByteBuffer buf = ByteBuffer.wrap(block, (int)offset,
            Math.min(block.length - (int)offset, Math.min((int)length, 8 * 1024 * 1024))); // 8 MB, for now
    return buf;
  }
}
