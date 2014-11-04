package org.apache.reef.inmemory.client;

import com.google.common.net.HostAndPort;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.service.SurfCacheService;
import org.apache.reef.inmemory.common.service.SurfMetaService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.*;

public class SurfFSOutputStream extends OutputStream {
  private final static int PACKET_SIZE = 512;
  private final Path path;
  private final long blockSize;

  private final byte localBuf[] = new byte[PACKET_SIZE];
  private int localBufWriteCount = 0;

  private AllocatedBlockInfo curAllocatedBlockInfo;
  private long curBlockOffset = 0;

  private final SurfMetaService.Client metaClient;
  private final Queue<Packet> packetQueue = new ConcurrentLinkedQueue<>();
  private final PacketStreamer streamer = new PacketStreamer();
  private final ExecutorService executor =
      new ThreadPoolExecutor(5, 5, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(80));

  public SurfFSOutputStream(Path path, SurfMetaService.Client metaClient, long blockSize) throws IOException, TException {
    this.blockSize = blockSize;
    this.metaClient = metaClient;
    this.path = path;
  }

  @Override
  public void write(int b) throws IOException {
    localBuf[localBufWriteCount++] = (byte)b;
    if (localBufWriteCount == PACKET_SIZE) {
      flushBuf(localBuf, 0, localBufWriteCount);
      localBufWriteCount = 0;
    }
 }

  // blocking???
  @Override
  public void flush() throws IOException {
    flushBuf(localBuf, 0, localBufWriteCount);
    localBufWriteCount = 0;

    while (packetQueue.size() > 0) {
      try {
        wait(1000);
      } catch (InterruptedException e) {
        // do not thrown an exception; just log.warning
      }
    }
  }

  @Override
  public void close() throws IOException {
    flush();
    this.executor.shutdown();
    // metaClient.completeFile(String path, offset, blockSize, NodeInfo lastNode);
  }

  private class PacketStreamer implements Runnable {
    @Override
    public void run() {
      try {
        final Packet packet = packetQueue.remove();
        final SurfCacheService.Client cacheClient = getCacheClient(packet.address);
        cacheClient.writeData(path, packet.blockCount, blockSize, packet.offset, ByteBuffer.wrap(packet.buf));
      } catch (Exception e) {
        throw new RuntimeException("PacketStreamer exception");
      }
    }
  }

  /**
   * @param b the buffer to flush
   * @param start inclusive
   * @param end exclusive
   */
  private void flushBuf(final byte[] b, final int start, final int end) {
    // Need to allocate/initialize a new block
    if (curBlockOffset == 0) {
      curAllocatedBlockInfo = metaClient.allocateBlock(path.toString(), curBlockOffset, blockSize, myaddress);
      final SurfCacheService.Client cacheClient = getCacheClient(curAllocatedBlockInfo);
      // cacheClient.initBlock(path, offset, blockSize, AllocatedBlockInfo)
    }

    if (curBlockOffset + (end - start) <= blockSize)  {
      sendPacket(curAllocatedBlockInfo, curBlockOffset, Arrays.copyOfRange(b, start, end));
      curBlockOffset = (curBlockOffset + (end - start)) % blockSize;
    } else {
      sendPacket(curAllocatedBlockInfo, curBlockOffset, Arrays.copyOfRange(b, start, end - (blockSize - curBlockOffset)));
      curBlockOffset = (curBlockOffset + (end - start)) % blockSize;
      flushBuf(Arrays.copyOfRange(end - (blockSize - curBlockOffset), end)); // flush the leftovers in b
    }
  }

  private void sendPacket(final AllocatedBlockInfo allocatedBlockInfo, final long offset, final byte[] buf) {
    packetQueue.add(new Packet(address, blockCount, offset, buf));
    this.executor.submit(streamer);
  }

  private SurfCacheService.Client getCacheClient(final String address) {
    try {
      final HostAndPort taskAddress = HostAndPort.fromString(address);
      final TTransport transport = new TFramedTransport(new TSocket(taskAddress.getHostText(), taskAddress.getPort()));
      transport.open();
      final TProtocol protocol = new TCompactProtocol(transport);
      return new SurfCacheService.Client(protocol);
    } catch (Exception e) {
      throw new RuntimeException("getCacheClient exception");
    }
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
}