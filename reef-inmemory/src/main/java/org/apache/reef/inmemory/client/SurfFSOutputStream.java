package org.apache.reef.inmemory.client;

import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.entity.WriteableBlockMeta;
import org.apache.reef.inmemory.common.service.SurfCacheService;
import org.apache.reef.inmemory.common.service.SurfMetaService;
import org.apache.thrift.TException;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * FSOutputStream for Surf.
 * Request MetaServer to allocate new blocks, on which it writes data.
 */
public final class SurfFSOutputStream extends OutputStream {
  private static final Logger LOG = Logger.getLogger(SurfFSOutputStream.class.getName());
  private static final int PACKET_SIZE = 4 * 1024 * 1024; // 4MB
  private static final int MAX_PACKETS = 80;
  private static final int COMPLETE_FILE_RETRY_NUM = 5;
  private static final int COMPLETE_FILE_RETRY_INTERVAL = 400;
  private static final int FLUSH_CHECK_INTERVAL = 100;

  private final SurfMetaService.Client metaClient;
  private final String path;
  private final long blockSize;
  private final String localAddress;

  private byte localBuf[] = new byte[PACKET_SIZE];
  private int localBufWriteCount = 0;
  private WriteableBlockMeta curWritableBlockMeta;
  private long curBlockOffset = 0;
  private long curBlockInnerOffset = 0;

  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private final BlockingQueue<Packet> packetQueue = new ArrayBlockingQueue<>(MAX_PACKETS); // shared with PacketStreamer
  private volatile boolean isClosed; // shared with PacketStreamer

  public SurfFSOutputStream(final String path,
                            final SurfMetaService.Client metaClient,
                            final CacheClientManager cacheClientManager,
                            final long blockSize) throws UnknownHostException {
    this.path = path;
    this.metaClient = metaClient;
    this.blockSize = blockSize;
    this.localAddress = InetAddress.getLocalHost().getHostName();
    this.isClosed = false;
    executor.execute(new PacketStreamer(cacheClientManager));
  }

  @Override
  public void write(final int b) throws IOException {
    localBuf[localBufWriteCount++] = (byte)b;
    if (localBufWriteCount == PACKET_SIZE) {
      flushLocalBuf(false);
    }
  }

  @Override
  public void write(final byte b[], final int off, final int len) throws IOException {
    if ((off < 0) || (len < 0) || ((off + len) > b.length)) {
      throw new IndexOutOfBoundsException();
    }

    // Fill out the local buffer until the contents of b is written.
    int startIndex = off;
    while (startIndex < len) {
      final int bytesLeftInUserBuf = len-startIndex;
      final int bytesLeftInLocalBuf = PACKET_SIZE - localBufWriteCount;
      final int length = Math.min(bytesLeftInUserBuf, bytesLeftInLocalBuf);

      System.arraycopy(b, startIndex, localBuf, localBufWriteCount, length);

      localBufWriteCount += length;
      startIndex += length;

      if (localBufWriteCount == PACKET_SIZE) {
        flushLocalBuf(false);
      }
    }
  }

  /**
   * Blocks until localBuf and packetQueue are emptied
   */
  @Override
  public void flush() throws IOException {
    flush(false);
  }

  /**
   * After flush(), notify SurfMetaServer of file completion
   */
  @Override
  public void close() throws IOException {
    // Confirms that the local queue is emptied
    flush(true);

    // Confirms that the meta server received all the block reports
    final long fileSize = blockSize * curBlockOffset + curBlockInnerOffset;
    boolean success = false;
    try {
      for (int i = 0; i < COMPLETE_FILE_RETRY_NUM; i++) {
        success = metaClient.completeFile(path, fileSize);
        if (success) {
          break;
        } else {
          Thread.sleep(COMPLETE_FILE_RETRY_INTERVAL);
        }
      }
    } catch (TException | InterruptedException e) {
      throw new IOException("Failed while closing the file", e);
    }
    if (!success) {
      throw new IOException("File not closed");
    }

    // Now we are safe to terminate the PacketStreamer thread
    this.isClosed = true;
    this.executor.shutdown();
  }

  private void flush(final boolean close) throws IOException {
    if (!nothingWasWritten()) { // if nothing got written at all, there's no need to send anything to a CacheServer
      flushLocalBuf(close);

      while (packetQueue.size() > 0) {
        try {
          Thread.sleep(FLUSH_CHECK_INTERVAL);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    }
  }

  private void flushLocalBuf(final boolean close) throws IOException {
    flushBuf(localBuf, 0, localBufWriteCount, close);
    localBuf = new byte[PACKET_SIZE]; // create a new localBuf, since flushBuf does not allow buf reuse
    localBufWriteCount = 0;
  }

  /**
   * The caller cannot reuse the buffer. To avoid copying, the buffer is wrapped.
   * If block overflow detected, create 2 packets by recursively calling itself.
   *
   * @param b the buffer to flush
   * @param start inclusive
   * @param end exclusive
   */
  private void flushBuf(final byte[] b, final int start, final int end, final boolean close) throws IOException {
    final int len = end - start;
    if (curBlockInnerOffset == 0 && len != 0) { // only when there's some more to write...
      curWritableBlockMeta = allocateBlockAtMetaServer(curBlockOffset);
    }

    if (curBlockInnerOffset + len < blockSize) {
      sendPacket(ByteBuffer.wrap(b, start, len), len, close);
    } else if (curBlockInnerOffset + len == blockSize) {
      sendPacket(ByteBuffer.wrap(b, start, len), len, true);
    } else {
      final int possibleLen = (int) (blockSize - curBlockInnerOffset); // this must be int because "possibleLen < len"
      sendPacket(ByteBuffer.wrap(b, start, possibleLen), possibleLen, true);
      flushBuf(b, start + possibleLen, end, close); // Create another packet with the leftovers
    }
  }

  private WriteableBlockMeta allocateBlockAtMetaServer(final long blockOffset) throws IOException {
    try {
      return metaClient.allocateBlock(path, blockOffset, localAddress);
    } catch (TException e) {
      throw new IOException("metaClient.allocateBlock failed", e);
    }
  }

  private void sendPacket(final ByteBuffer buf,
                          final int len,
                          final boolean isLastPacket) throws IOException {
    try {
      packetQueue.put(new Packet(curWritableBlockMeta, curBlockInnerOffset, buf, isLastPacket));
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    updateOffsets(curBlockInnerOffset + len);
  }

  private void updateOffsets(final long totalInnerOffset) {
    curBlockOffset += totalInnerOffset / blockSize;
    curBlockInnerOffset = totalInnerOffset % blockSize;
  }

  private static class Packet {
    final WriteableBlockMeta writeableBlockMeta;
    final long blockInnerOffset;
    final ByteBuffer buf;
    final boolean isLastPacket;

    public Packet(final WriteableBlockMeta writeableBlockMeta,
                  final long blockInnerOffset,
                  final ByteBuffer buf,
                  final boolean isLastPacket) {
      this.writeableBlockMeta = writeableBlockMeta;
      this.blockInnerOffset = blockInnerOffset;
      this.buf = buf;
      this.isLastPacket = isLastPacket;
    }
  }

  private class PacketStreamer implements Runnable {
    final CacheClientManager cacheClientManager;
    SurfCacheService.Client curCacheClient;

    PacketStreamer(final CacheClientManager cacheClientManager) {
      this.cacheClientManager = cacheClientManager;
    }

    @Override
    public void run() {
      while(!isClosed) {
        try {
          final Packet packet = packetQueue.take();
          if (packet.blockInnerOffset == 0 && packet.buf.limit() > 0) { // only when we need to actually write sth...
            // Initialize block at the cache server for the first packet of a block
            initCacheClient(packet.writeableBlockMeta);
          }

          final BlockMeta blockMeta = packet.writeableBlockMeta.getBlockMeta();
          curCacheClient.writeData(blockMeta.getFileId(), blockMeta.getOffSet(), packet.blockInnerOffset, packet.buf, packet.isLastPacket);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    private void initCacheClient(final WriteableBlockMeta writableBlockMeta) {
      boolean success = false;
      for (final NodeInfo nodeInfo : writableBlockMeta.getBlockMeta().getLocations()) {
        try {
          curCacheClient = cacheClientManager.get(nodeInfo.getAddress());
          curCacheClient.initBlock(blockSize, writableBlockMeta);
          success = true;
          break;
        } catch (TException e) {
          LOG.log(Level.WARNING, "Cache Server is not responding... trying the next one(if there is any left)", e);
        }
      }

      if (!success) {
        throw new RuntimeException("None of the cache nodes is responding");
      }
    }
  }

  private boolean nothingWasWritten() {
    return localBufWriteCount == 0 && curBlockOffset == 0 && curBlockInnerOffset == 0;
  }

  // Methods used for unit tests.
  protected static int getPacketSize() {
    return PACKET_SIZE;
  }

  protected int getLocalBufWriteCount() {
    return localBufWriteCount;
  }

  protected long getCurBlockInnerOffset() {
    return curBlockInnerOffset;
  }

  protected long getCurBlockOffset() {
    return curBlockOffset;
  }
}