package org.apache.reef.inmemory.task.service;

import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.common.entity.WriteableBlockMeta;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;
import org.apache.reef.inmemory.common.exceptions.BlockWritingException;
import org.apache.reef.inmemory.common.instrumentation.Event;
import org.apache.reef.inmemory.common.instrumentation.EventRecorder;
import org.apache.reef.inmemory.common.service.SurfCacheService;
import org.apache.reef.inmemory.task.BlockWriter;
import org.apache.reef.inmemory.task.CacheParameters;
import org.apache.reef.inmemory.task.InMemoryCache;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.thrift.TException;
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
  private final EventRecorder RECORD;

  private final InMemoryCache cache;
  private final int port;
  private final int timeout;
  private final int numThreads;
  private final int bufferSize;

  private TServer server = null;
  private int bindPort;

  @Inject
  public SurfCacheServer(final InMemoryCache cache,
                         @Parameter(CacheParameters.Port.class) final int port,
                         @Parameter(CacheParameters.Timeout.class) final int timeout,
                         @Parameter(CacheParameters.NumServerThreads.class) final int numThreads,
                         @Parameter(CacheParameters.LoadingBufferSize.class) final int bufferSize,
                         final EventRecorder recorder) {
    this.cache = cache;
    this.port = port;
    this.timeout = timeout;
    this.numThreads = numThreads;
    this.bufferSize = bufferSize;
    this.RECORD = recorder;
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
      if (this.server != null && this.server.isServing()) {
        this.server.stop();
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (this.server != null && this.server.isServing()) {
      this.server.stop();
    }
  }

  @Override
  public ByteBuffer getData(final BlockMeta blockMeta, final long offset, final long length)
          throws BlockLoadingException, BlockNotFoundException, BlockWritingException {
    final Event getDataEvent = RECORD.event("task.get-data",
            blockMeta.toString() + ":" + Long.toString(offset)).start();
    final BlockId blockId = new BlockId(blockMeta);

    // The first and last index to load blocks
    final int chunkIndex = (int) offset / bufferSize;
    final int chunkOffset = ((int) offset) % bufferSize;

    final byte[] chunk = cache.get(blockId, chunkIndex);

    final ByteBuffer buf = ByteBuffer.wrap(chunk, chunkOffset,
            Math.min(chunk.length - chunkOffset, (int) Math.min(Integer.MAX_VALUE, length)));
    RECORD.record(getDataEvent.stop());
    return buf;
  }


  // TODO: initBlock may not be necessary (simply use the code for the first packet of the block in writeData)
  @Override
  public void initBlock(final long blockSize, final WriteableBlockMeta writableBlockMeta) throws TException {
    /*
     * Create a cache entry (BlockLoader) and load it into the cache
     * so the cache can receive the data or write the data into memory
     */
    final BlockMeta blockMeta = writableBlockMeta.getBlockMeta();
    final BlockId blockId = new BlockId(blockMeta);
    final boolean pin = writableBlockMeta.isPin();
    final BlockWriter blockWriter = new BlockWriter(blockId, blockSize, pin, bufferSize);

    try {
      cache.prepareToWrite(blockWriter);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception while initializing ", e);
      throw new TException("Failed to initialize a block", e);
    }
  }

  // TODO: replace this synchronous call with an asynchronous protocol for better performance
  @Override
  public void writeData(final long fileId, final long blockOffset, final long innerOffset, final ByteBuffer buf,
                        final boolean isLastPacket) throws TException {
    final BlockId blockId = new BlockId(fileId, blockOffset);
    try {
      cache.write(blockId, innerOffset, buf, isLastPacket);
    } catch (IOException e) {
      throw new TException("Failed to write block " + blockId, e);
    }
  }
}
