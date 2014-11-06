package org.apache.reef.inmemory.client;

import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.service.SurfCacheService;
import org.apache.reef.inmemory.common.service.SurfMetaService;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
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

  private final byte localBuf[] = new byte[PACKET_SIZE];
  private int localBufWriteCount = 0;
  private AllocatedBlockInfo curAllocatedBlockInfo;
  private long curBlockOffset = 0;

  private final SurfMetaService.Client metaClient;
  private final CacheClientManager cacheClientManager;

  private final Queue<Packet> packetQueue = new ConcurrentLinkedQueue<>();
  private final PacketStreamer streamer = new PacketStreamer();
  private final ExecutorService executor =
      new ThreadPoolExecutor(5, 5, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(80));

  public SurfFSOutputStream(Path path,
                            SurfMetaService.Client metaClient,
                            CacheClientManager cacheClientManager,
                            long blockSize) {
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
      flushBuf(localBuf, 0, localBufWriteCount);
      localBufWriteCount = 0;
    }
 }

  @Override
  public void flush() throws IOException {
    flushBuf(localBuf, 0, localBufWriteCount);
    localBufWriteCount = 0;

    while (packetQueue.size() > 0) {
      try {
        wait(1000);
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "flush() sleep interrupted. Sleeping again...");
      }
    }
  }

  @Override
  public void close() throws IOException {
    flush();
    this.executor.shutdown();
    metaClient.completeFile(String path, offset, blockSize, NodeInfo lastNode);
  }

  /**
   * @param b the buffer to flush
   * @param start inclusive
   * @param end exclusive
   */
  private void flushBuf(final byte[] b, final int start, final int end) {
    final int len = end - start;

    if (curBlockOffset == 0) {
      curAllocatedBlockInfo = metaClient.allocateBlock(path.toString(), curBlockOffset, blockSize, localAddress);
      final SurfCacheService.Client cacheClient = cacheClientManager.get(curAllocatedBlockInfo);
      cacheClient.initBlock(path, offset, blockSize, AllocatedBlockInfo)
    }

    if (curBlockOffset + len) <= blockSize)  {
      sendPacket(curAllocatedBlockInfo, curBlockOffset, Arrays.copyOfRange(b, start, end));
      curBlockOffset = (curBlockOffset + (end - start)) % blockSize;
    } else {
      sendPacket(curAllocatedBlockInfo, curBlockOffset, Arrays.copyOfRange(b, start, start + (blockSize - curBlockOffset)));
      curBlockOffset = (curBlockOffset + (end - start)) % blockSize;
      flushBuf(Arrays.copyOfRange(end - (blockSize - curBlockOffset), end)); // flush the leftovers in b
    }
  }

  private void sendPacket(final AllocatedBlockInfo allocatedBlockInfo, final long offset, final byte[] buf) {
    packetQueue.add(new Packet(address, blockCount, offset, buf));
    this.executor.submit(streamer);
  }

  private class Packet {
    final AllocatedBlockInfo allocatedBlockInfo;
    final long blockCount;
    final long offset;
    final byte[] buf;

    public Packet(final AllocatedBlockInfo allocatedBlockInfo, final long blockCount, final long offset, final byte[] buf) {
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
        final SurfCacheService.Client cacheClient = cacheClientManager.get(packet.address);
        cacheClient.writeData(path, packet.blockCount, blockSize, packet.offset, ByteBuffer.wrap(packet.buf));
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

  public AllocatedBlockInfo getCurAllocatedBlockInfo() {
    return curAllocatedBlockInfo;
  }

  public int getPacketSize() {
    return PACKET_SIZE;
  }
}