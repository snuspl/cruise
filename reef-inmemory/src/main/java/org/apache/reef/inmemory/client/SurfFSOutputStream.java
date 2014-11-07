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

  private final Path path;
  private final long blockSize;
  private final String localAddress;

  private byte localBuf[] = new byte[PACKET_SIZE];
  private int localBufWriteCount = 0;
  private AllocatedBlockInfo curAllocatedBlockInfo;
  private int curBlockOffset = 0;

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
      flushLocalBuf();
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
        flushLocalBuf();
      }
    }
  }

  /**
   * Blocks until localBuf and packetQueue are emptied
   */
  @Override
  public void flush() throws IOException {
    if (localBufWriteCount > 0) {
      flushLocalBuf();
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

  /**
   * After flush(), notify SurfMetaServer of file completion
   */
  @Override
  public void close() throws IOException {
    flush();
    this.executor.shutdown();
    // TODO resolve the lastNode
    NodeInfo lastNode = null;
    try {
      metaClient.completeFile(path.toString(), curBlockOffset, blockSize, lastNode);
    } catch (TException e) {
      throw new IOException("Failed while closing the file", e);
    }
  }

  private void flushLocalBuf() throws IOException {
    flushBuf(localBuf, 0, localBufWriteCount);
    localBuf = new byte[PACKET_SIZE]; // create a new localBuf since the localBuf cannot be reused
    localBufWriteCount = 0;
  }

  /**
   * To avoid copying, the buffer is wrapped. The caller cannot reuse the buffer.
   * If block overflow detected, create 2 packets by recursively calling itself.
   *
   * @param b the buffer to flush
   * @param start inclusive
   * @param end exclusive
   */
  private void flushBuf(final byte[] b, final int start, final int end) throws IOException {
    final int len = end - start;

    if (curBlockOffset == 0) {
      initNewBlock();
    }

    // TODO Make sure we set right offset here
    if (curBlockOffset + len <= blockSize)  {
      sendPacket(curAllocatedBlockInfo, curBlockOffset, ByteBuffer.wrap(b, start, len));
      curBlockOffset = (curBlockOffset + len) % (int)blockSize;
    } else {
      // TODO This is to be integer value.
      final int possibleLen = (int)blockSize - curBlockOffset;
      sendPacket(curAllocatedBlockInfo, curBlockOffset, ByteBuffer.wrap(b, start, possibleLen));
      curBlockOffset = 0;

      flushBuf(b, start+possibleLen, end); // Create another packet with the leftovers
    }
  }

  private void initNewBlock() throws IOException {
    try {
      curAllocatedBlockInfo = metaClient.allocateBlock(path.toString(), curBlockOffset, blockSize, localAddress);
      // TODO Make sure it is the right address
      final String address = curAllocatedBlockInfo.getLocations().get(0).getAddress();
      final SurfCacheService.Client cacheClient = cacheClientManager.get(address);
      // TODO resolve the value of offset
      long offset = -1;
      cacheClient.initBlock(path.toString(), offset, blockSize, curAllocatedBlockInfo);
    } catch (TException e) {
      throw new IOException("Failed to initialize a block", e);
    }
  }

  private void sendPacket(final AllocatedBlockInfo allocatedBlockInfo, final long offset, final ByteBuffer buf) {
    // TODO Resolve the blockCount
    long blockCount = -1;
    packetQueue.add(new Packet(allocatedBlockInfo, blockCount, offset, buf));
    this.executor.submit(streamer);
  }

  private class Packet {
    final AllocatedBlockInfo allocatedBlockInfo;
    final long blockCount;
    final long offset;
    final ByteBuffer buf;

    public Packet(final AllocatedBlockInfo allocatedBlockInfo,
                  final long blockCount,
                  final long offset,
                  final ByteBuffer buf) {
      this.allocatedBlockInfo = allocatedBlockInfo;
      this.blockCount = blockCount;
      this.offset = offset;
      this.buf = buf;
    }
  }

  private class PacketStreamer implements Runnable {
    @Override
    public void run() {
      try {
        final Packet packet = packetQueue.remove();
        final String address = packet.allocatedBlockInfo.getLocations().get(0).getAddress();
        final SurfCacheService.Client cacheClient = cacheClientManager.get(address);
        // TODO Check if it is the last packet
        boolean isLastPacket = false;
        cacheClient.writeData(path.toString(), packet.blockCount, blockSize, packet.offset, packet.buf, isLastPacket);
      } catch (Exception e) {
        throw new RuntimeException("PacketStreamer exception");
      }
    }
  }

  public int getLocalBufWriteCount() {
    return localBufWriteCount;
  }

  public long getCurBlockOffset() {
    return curBlockOffset;
  }

  public int getPacketSize() {
    return PACKET_SIZE;
  }
}