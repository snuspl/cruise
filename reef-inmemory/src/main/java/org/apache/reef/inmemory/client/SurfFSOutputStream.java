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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Output stream implementation. Register metadata when created,
 * and as Client writes the data allocate blocks and transfer the
 * data to the target block. When it fills the block, update metadata
 * and repeat this step until close is called.
 */
public class SurfFSOutputStream extends OutputStream {
  // 512B(packet size) X 80(queue size) = 40KB
  private final static int PACKET_SIZE = 512;
  private final static int QUEUE_MAX_SIZE = 80;

  private final DataStreamer streamer = new DataStreamer();

  private final long blockSize;
  private final SurfMetaService.Client metaClient;
  private final Path path;

  private final byte localBuf[] = new byte[PACKET_SIZE];
  private int totalWriteCount = 0;
  private final Queue<Packet> packetQueue = new ConcurrentLinkedQueue<>();

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
    localBuf[totalWriteCount++] = (byte)b;
    if (totalWriteCount % localBuf.length == 0) {
      flush();
    }
 }

  @Override
  public void flush() throws IOException {
    synchronized (this) {
      final long blockId = path + offset;
      final long offset = totalWriteCount % localBuf.length;
      final byte[] buf = localBuf.clone();
      packetQueue.add(new Packet(blockId, offset, buf));
    }
  }

  @Override
  public void close() throws IOException {
    // TODO Investigate what a proper action is
    flush();
  }

  private class DataStreamer implements Runnable {
    @Override
    public void run() {
      while(true) {
        if (packetQueue.size() != 0) {
          try {
            final String address = metaClient.allocateBlock(path.toString());
            final HostAndPort taskAddress = HostAndPort.fromString(address);
            final TTransport transport = new TFramedTransport(new TSocket(taskAddress.getHostText(), taskAddress.getPort()));
            transport.open();
            final TProtocol protocol = new TCompactProtocol(transport);
            SurfCacheService.Client cacheClient = new SurfCacheService.Client(protocol);

            Packet packet = packetQueue.remove();
            cacheClient.writeData(packet.blockId, packet.offset, ByteBuffer.wrap(packet.buf));
          } catch (TException e) {
          }
        }
      }
    }
  }

  private class Packet {
    final long blockId;
    final long offset;
    final byte[] buf;

    public Packet(final long blockId, final long offset, final byte[] buf) {
      this.blockId = blockId;
      this.offset = offset;
      this.buf = buf;
    }
  }
}
