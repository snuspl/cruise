package org.apache.reef.inmemory.client;

import org.apache.hadoop.fs.Path;
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
import java.util.Queue;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SurfFSOutputStream extends OutputStream {
  private static final Logger LOG = Logger.getLogger(SurfFSOutputStream.class.getName());
  private final static int PACKET_SIZE = 512;

  private final String path;
  private final long blockSize;
  private final String localAddress;

  private byte localBuf[] = new byte[PACKET_SIZE];
  private int localBufWriteCount = 0;
  private long curBlockOffset = 0;
  private long curBlockInnerOffset = 0;

  private final Queue<Packet> packetQueue = new ConcurrentLinkedQueue<>(); // add by main thread, remove by PacketStreamer

  private final SurfMetaService.Client metaClient;
  private final CacheClientManager cacheClientManager;

  private final PacketStreamer streamer = new PacketStreamer();
  private final ExecutorService executor =
    new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(80)); // max 81 packets
  private String curCacheAddress;

  public SurfFSOutputStream(final String path,
                            final SurfMetaService.Client metaClient,
                            final CacheClientManager cacheClientManager,
                            final long blockSize) throws UnknownHostException {
    this.path = path;
    this.metaClient = metaClient;
    this.cacheClientManager = cacheClientManager;
    this.blockSize = blockSize;
    this.localAddress = InetAddress.getLocalHost().getHostName();
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
    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) ||
      ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

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
      flush(true);
      this.executor.shutdown();

      final long fileSize = blockSize * curBlockOffset + curBlockInnerOffset;
      if (fileSize > 0) {
        boolean success = false;
        try {
          for (int i = 0; i < 3; i++) {
            success = metaClient.completeFile(path, fileSize);
            if (success) {
              break;
            } else {
              Thread.sleep(5000); // TODO: make it 1 second
            }
          }
        } catch (TException | InterruptedException e) {
          throw new IOException("Failed while closing the file", e);
        }

        if (!success) {
          throw new IOException("File not closed");
        }
      }
    }
  }

  public void flush(final boolean isLastPacket) throws IOException {
    if (localBufWriteCount > 0) {
      flushLocalBuf(isLastPacket);
    }

    synchronized (this) {
      while (packetQueue.size() > 0) {
        try {
          wait(1000);
        } catch (InterruptedException e) {
          LOG.log(Level.WARNING, "flush() sleep interrupted");
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
      // the first packet of a block
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

  private void sendPacket(final AllocatedBlockInfo blockInfo, final ByteBuffer buf, final int len, final boolean isLastPacket) {
    packetQueue.add(new Packet(blockInfo, curBlockOffset, curBlockInnerOffset, buf, isLastPacket));
    this.executor.submit(streamer);
    updateBlockOffsets(curBlockInnerOffset+len);
  }

  private void updateBlockOffsets(long totalOffset) {
    curBlockOffset += totalOffset / blockSize;
    curBlockInnerOffset = totalOffset % blockSize;
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
    @Override
    public void run() {
      try {
        final Packet packet = packetQueue.remove();
        if (packet.blockInfo != null) {
          curCacheAddress = initBlockAtCacheServer(packet.blockInfo, packet.blockOffset); // the first packet of a block
        }
        final SurfCacheService.Client cacheClient = cacheClientManager.get(curCacheAddress);
        cacheClient.writeData(path, packet.blockOffset, blockSize, packet.blockInnerOffset, packet.buf, packet.isLastPacket);
      } catch (Exception e) {
        throw new RuntimeException("PacketStreamer exception");
      }
    }

    private String initBlockAtCacheServer(final AllocatedBlockInfo blockInfo, final long blockOffset) {
      String cacheAddress = null;
      for (final NodeInfo nodeInfo : blockInfo.getLocations()) {
        try {
          final SurfCacheService.Client cacheClient = cacheClientManager.get(nodeInfo.getAddress());
          cacheClient.initBlock(path, blockOffset, blockSize, blockInfo);
          cacheAddress = nodeInfo.getAddress();
          break;
        } catch (TException e) {
          LOG.log(Level.WARNING, "Cache Server is not responding... trying the next one(if there is any left)");
        }
      }

      if (cacheAddress == null) {
        throw new RuntimeException("None of the cache nodes is responding");
      } else {
        return cacheAddress;
      }
    }
  }

  public int getLocalBufWriteCount() {
    return localBufWriteCount;
  }

  public long getCurBlockInnerOffset() {
    return curBlockInnerOffset;
  }

  public int getPacketSize() {
    return PACKET_SIZE;
  }

  public long getCurBlockOffset() {
    return curBlockOffset;
  }
}