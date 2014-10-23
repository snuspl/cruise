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

/**
 * Output stream implementation. Register metadata when created,
 * and as Client writes the data allocate blocks and transfer the
 * data to the target block. When it fills the block, update metadata
 * and repeat this step until close is called.
 */
public class SurfFSOutputStream extends OutputStream {
  private final SurfMetaService.Client metaClient;
  private final Path path;

  private String cachedBlockAddress;
  private final long blockSize;
  private int blockOffset = 0;

  private final byte localBuf[] = new byte[PACKET_SIZE];
  private int localBufWriteCount = 0;

  private final static int PACKET_SIZE = 512;
  private final Queue<Packet> packetQueue = new ConcurrentLinkedQueue<>();
  private final PacketStreamer streamer = new PacketStreamer();
  private final ExecutorService executor =
      new ThreadPoolExecutor(5, 5, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(80));

  /**
   * This constructor is called outside with the information to create a file
   * @throws IOException If the file exists already
   */
  public SurfFSOutputStream(Path path, SurfMetaService.Client metaClient, long blockSize) throws IOException, TException {
    this.blockSize = blockSize;
    this.metaClient = metaClient;
    this.path = path;
  }

  @Override
  public void write(int b) throws IOException {
    localBuf[localBufWriteCount++] = (byte)b;
    if (localBufWriteCount == PACKET_SIZE) {
      flush();
    }
 }

  // blocking???
  @Override
  public void flush() throws IOException {
    synchronized (this) {
      // do metaClient.allocateBlock and handle boundary cases
      if (localBufWriteCount != 0) {
        if (blockOffset + localBufWriteCount <= blockSize)  {
          sendPacket(cachedBlockAddress, blockOffset, Arrays.copyOfRange(localBuf, 0, localBufWriteCount));
        } else {
          final String nextAddress = metaClient.allocateBlock(path.toString(), blockSize);
          sendPacket(cachedBlockAddress, blockOffset, Arrays.copyOfRange(localBuf, 0, blockSize-blockOffset));
          sendPacket(nextAddress, 0, Arrays.copyOfRange(localBuf, localBuf.length+blockOffset-blockSize, localBuf.length));
          cachedBlockAddress = nextAddress;
        }

        blockOffset = (blockOffset + localBufWriteCount) % blockSize;
        localBufWriteCount = 0;
      }
    }
  }

  @Override
  public void close() throws IOException {
    flush();
    this.executor.shutdown();
  }

  private class PacketStreamer implements Runnable {
    @Override
    public void run() {
      try {
        final Packet packet = packetQueue.remove();
        final HostAndPort taskAddress = HostAndPort.fromString(packet.address);
        final TTransport transport = new TFramedTransport(new TSocket(taskAddress.getHostText(), taskAddress.getPort()));
        transport.open();
        final TProtocol protocol = new TCompactProtocol(transport);
        final SurfCacheService.Client cacheClient = new SurfCacheService.Client(protocol);

        cacheClient.writeData(path, packet.offset, ByteBuffer.wrap(packet.buf));
      } catch (Exception e) {
        throw new RuntimeException("PacketStreamer exception");
      }
    }
  }

  private class Packet {
    final String address;
    final long offset;
    final byte[] buf;

    public Packet(final String address, final long offset, final byte[] buf) {
      this.address = address;
      this.offset = offset;
      this.buf = buf;
    }
  }

  private void sendPacket(final String address, final long offset, final byte[] buf) {
    packetQueue.add(new Packet(address, offset, buf));
    this.executor.submit(streamer);
  }
}
