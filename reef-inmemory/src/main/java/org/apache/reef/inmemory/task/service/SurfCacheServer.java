package org.apache.reef.inmemory.task.service;

import com.microsoft.tang.annotations.Parameter;
import org.apache.reef.inmemory.common.BlockIdFactory;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;
import org.apache.reef.inmemory.common.service.SurfCacheService;
import org.apache.reef.inmemory.task.BlockId;
import org.apache.reef.inmemory.task.CacheParameters;
import org.apache.reef.inmemory.task.InMemoryCache;
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
 * Serves block data in task to Surf clients, using Thrift.
 */
public final class SurfCacheServer implements SurfCacheService.Iface, Runnable, AutoCloseable {

  private static final Logger LOG = Logger.getLogger(SurfCacheServer.class.getName());

  private final InMemoryCache cache;
  private final BlockIdFactory blockIdFactory;
  private final int port;
  private final int timeout;
  private final int numThreads;
  private final int bufferSize;

  private TServer server = null;
  private int bindPort;

  @Inject
  public SurfCacheServer(final InMemoryCache cache,
                         final BlockIdFactory blockIdFactory,
                         final @Parameter(CacheParameters.Port.class) int port,
                         final @Parameter(CacheParameters.Timeout.class) int timeout,
                         final @Parameter(CacheParameters.NumServerThreads.class) int numThreads,
                         final @Parameter(CacheParameters.LoadingBufferSize.class) int bufferSize) {
    this.cache = cache;
    this.blockIdFactory = blockIdFactory;
    this.port = port;
    this.timeout = timeout;
    this.numThreads = numThreads;
    this.bufferSize = bufferSize;
  }

  public int getBindPort() {
    return this.bindPort;
  }

  /**
   * Initialize the port to bind. If task port is 0, will return a new ephemeral port.
   * If task port is specified, it will return the port.
   *
   * The ephemeral port is not guaranteed to be open when run is called.
   * But must do this because Thrift will not return a bound port number.
   */
  public int initBindPort() throws IOException {
    if (this.port == 0) {
      final ServerSocket reservation = new ServerSocket(0, 1);
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
      final TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(this.bindPort, this.timeout);

      final SurfCacheService.Processor<SurfCacheService.Iface> processor =
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
  public ByteBuffer getData(final BlockInfo blockInfo, final long offset, final long length)
    throws BlockLoadingException, BlockNotFoundException {
    final BlockId blockId = blockIdFactory.newBlockId(blockInfo);

    // The first and last index to load blocks
    final int indexStart = (int)offset / bufferSize;

    int nWrite = 0;
    ByteBuffer buf = ByteBuffer.allocate((int)length);
    for(int i = indexStart; i * bufferSize < (int)(offset + length); i++) {
      byte[] temp = cache.get(blockId, i);

      int startOffset = (i == indexStart) ? (int)offset % bufferSize : 0;
      buf.put(temp, startOffset, temp.length);
      nWrite += temp.length;
    }

    /*
     * We need to limit the size of ByteBuffer into the size of actual file.
     * Otherwise when {@code length} is larger than actual file size, it could cause an Exception
     * while reading the data using this ByteBuffer.
     */
    buf.limit(nWrite).position(0);
    return buf;
  }
}