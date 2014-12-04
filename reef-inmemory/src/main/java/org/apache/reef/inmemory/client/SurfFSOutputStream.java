package org.apache.reef.inmemory.client;

import org.apache.reef.inmemory.common.entity.AllocatedBlockInfo;
import org.apache.reef.inmemory.common.entity.NodeInfo;
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

public class SurfFSOutputStream extends OutputStream {
  private static final Logger LOG = Logger.getLogger(SurfFSOutputStream.class.getName());
  private static final int PACKET_SIZE = 512;
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
  public void write(int b) throws IOException {
    localBuf[localBufWriteCount++] = (byte)b;
    if (localBufWriteCount == PACKET_SIZE) {
      flushLocalBuf(false);
    }
  }

  @Override
  public void write(byte b[], int off, int len) throws IOException {
    // Check the arguments
    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) ||
      ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
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
    synchronized (this) {
      // Confirms that the local queue is emptied
      flush(true);

      // Confirms that the meta server received all the block reports
      final long fileSize = blockSize * curBlockOffset + curBlockInnerOffset;
      if (fileSize > 0) {
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
      }

      // Now we are safe to terminate the PacketStreamer thread
      this.isClosed = true;
      this.executor.shutdown();
    }
  }

  public void flush(final boolean isLastPacket) throws IOException {
    if (localBufWriteCount > 0) {
      flushLocalBuf(isLastPacket);
    }

    synchronized (this) {
      while (packetQueue.size() > 0) {
        try {
          wait(FLUSH_CHECK_INTERVAL);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    }
  }

  private void flushLocalBuf(final boolean isLastPacket) throws IOException {
    flushBuf(localBuf, 0, localBufWriteCount, isLastPacket);
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
  private void flushBuf(final byte[] b, final int start, final int end, final boolean isLastPacket) throws IOException {
    final int len = end - start;

    AllocatedBlockInfo blockInfo = null;
    if (curBlockInnerOffset == 0) {
      // Request allocation for the first packet of a block
      blockInfo = allocateBlockAtMetaServer(curBlockOffset);
    }

    if (curBlockInnerOffset + len < blockSize) {
      sendPacket(blockInfo, ByteBuffer.wrap(b, start, len), len, isLastPacket);
    } else if (curBlockInnerOffset + len == blockSize) {
      sendPacket(blockInfo, ByteBuffer.wrap(b, start, len), len, true);
    } else {
      final int possibleLen = (int) (blockSize - curBlockInnerOffset); // this must be int because "possibleLen <= len"
      sendPacket(blockInfo, ByteBuffer.wrap(b, start, possibleLen), possibleLen, true);
      flushBuf(b, start + possibleLen, end, isLastPacket); // Create another packet with the leftovers
    }
  }

  private AllocatedBlockInfo allocateBlockAtMetaServer(final long blockOffset) throws IOException {
    try {
      return metaClient.allocateBlock(path, blockOffset, localAddress);
    } catch (TException e) {
      throw new IOException("metaClient.allocateBlock failed", e);
    }
  }

  private void sendPacket(final AllocatedBlockInfo blockInfo,
                          final ByteBuffer buf,
                          final int len,
                          final boolean isLastPacket) throws IOException {
    try {
      packetQueue.put(new Packet(blockInfo, curBlockOffset, curBlockInnerOffset, buf, isLastPacket));
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    updateOffsets(curBlockInnerOffset + len);
  }

  private void updateOffsets(final long totalInnerOffset) {
    curBlockOffset += totalInnerOffset / blockSize;
    curBlockInnerOffset = totalInnerOffset % blockSize;
  }

  private class Packet {
    final AllocatedBlockInfo blockInfo;
    final long blockOffset;
    final long blockInnerOffset;
    final ByteBuffer buf;
    final boolean isLastPacket;

    public Packet(final AllocatedBlockInfo blockInfo,
                  final long blockOffset,
                  final long blockInnerOffset,
                  final ByteBuffer buf,
                  final boolean isLastPacket) {
      this.blockInfo = blockInfo;
      this.blockOffset = blockOffset;
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
          if (packet.blockInfo != null) {
            // Initialize block at the cache server for the first packet of a block
            initBlockAtCacheServer(packet.blockInfo, packet.blockOffset);
          }
          curCacheClient.writeData(path, packet.blockOffset, blockSize, packet.blockInnerOffset, packet.buf, packet.isLastPacket);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    private void initBlockAtCacheServer(final AllocatedBlockInfo blockInfo, final long blockOffset) {
      boolean success = false;
      for (final NodeInfo nodeInfo : blockInfo.getLocations()) {
        try {
          curCacheClient = cacheClientManager.get(nodeInfo.getAddress());
          curCacheClient.initBlock(path, blockOffset, blockSize, blockInfo);
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

  // Methods used for unit tests.

  protected int getLocalBufWriteCount() {
    return localBufWriteCount;
  }

  protected long getCurBlockInnerOffset() {
    return curBlockInnerOffset;
  }

  protected int getPacketSize() {
    return PACKET_SIZE;
  }

  protected long getCurBlockOffset() {
    return curBlockOffset;
  }
}