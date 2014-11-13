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

  private NodeInfo curBlockNodeInfo;
  private long curBlockOffset = -1;
  private long curBlockInnerOffset = 0;

  private final SurfMetaService.Client metaClient;
  private final CacheClientManager cacheClientManager;

  private final Queue<Packet> packetQueue = new ConcurrentLinkedQueue<>();
  private final PacketStreamer streamer = new PacketStreamer();
  private final ExecutorService executor =
    new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(80)); // max 81 packets

  public SurfFSOutputStream(final Path path,
                            final SurfMetaService.Client metaClient,
                            final CacheClientManager cacheClientManager,
                            final long blockSize) throws UnknownHostException {
    this.path = path.toString();
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
    flush(true);
    this.executor.shutdown();
    try {
      metaClient.completeFile(path, curBlockInnerOffset, blockSize, curBlockNodeInfo);
    } catch (TException e) {
      throw new IOException("Failed while closing the file", e);
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

    if (curBlockInnerOffset == 0) {
      initNewBlock();
    }

    if (curBlockInnerOffset + len <= blockSize)  {
      sendPacket(curBlockNodeInfo.getAddress(), ByteBuffer.wrap(b, start, len), len, isLastPacket);
    } else {
      final int possibleLen = (int)(blockSize - curBlockInnerOffset); // this must be int because "possibleLen <= len"
      sendPacket(curBlockNodeInfo.getAddress(), ByteBuffer.wrap(b, start, possibleLen), possibleLen, false);

      flushBuf(b, start+possibleLen, end, isLastPacket); // Create another packet with the leftovers
    }
  }

  private void initNewBlock() throws IOException {
    try {

      curBlockOffset++;

      final AllocatedBlockInfo blockInfo = metaClient.allocateBlock(path, curBlockOffset, blockSize, localAddress);

      boolean success = false;
      for (final NodeInfo nodeInfo : blockInfo.getLocations()) {
        final SurfCacheService.Client cacheClient = cacheClientManager.get(nodeInfo.getAddress());
        try {
          cacheClient.initBlock(path, curBlockOffset, blockSize, blockInfo);
          curBlockNodeInfo = nodeInfo;
          success = true;
          break;
        } catch (TException e) {
          LOG.log(Level.WARNING, "Cache {0} is not responding... trying the next one", nodeInfo);
        }
      }
      if (!success) {
        throw new IOException("None of the cache nodes is responding");
      }

    } catch (TException e) {
      throw new IOException("metaClient.allocateBlock failed", e);
    }
  }

  private void sendPacket(final String address, final ByteBuffer buf, final int len, final boolean isLastPacket) {
    packetQueue.add(new Packet(address, curBlockOffset, curBlockInnerOffset, buf, isLastPacket));
    this.executor.submit(streamer);
    curBlockInnerOffset = (curBlockInnerOffset + len) % blockSize;
  }

  private class Packet {
    final String cacheAddress;
    final long blockOffset;
    final long blockInnerOffset;
    final ByteBuffer buf;
    final boolean isLastPacket;

    public Packet(final String cacheAddress,
                  final long blockOffset,
                  final long blockInnerOffset,
                  final ByteBuffer buf,
                  final boolean isLastPacket) {
      this.cacheAddress = cacheAddress;
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
        final SurfCacheService.Client cacheClient = cacheClientManager.get(packet.cacheAddress);
        cacheClient.writeData(path, packet.blockOffset, blockSize, packet.blockInnerOffset, packet.buf, packet.isLastPacket);
      } catch (Exception e) {
        throw new RuntimeException("PacketStreamer exception");
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
}